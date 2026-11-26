# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# DO NOT MERGE: CI memory sampler for a test-resource experiment.
# `start` forks a background sampler that records system memory and every Java process (role + RSS) every 10 s.
# `report` prints a summary to the job log: cgroup memory limits and OOM counters, kernel OOM-killer lines,
# a per-minute timeline and per-role peaks, then pushes the raw samples as a job artifact.
import json, os, re, subprocess, sys, time

OUT = os.environ.get('MEM_SAMPLER_OUT', '/tmp/mem-sampler.jsonl')
PID = OUT + '.pid'
INTERVAL = 10


def meminfo():
    m = {}
    for l in open('/proc/meminfo'):
        k, v = l.split(':', 1)
        m[k] = int(v.split()[0]) // 1024
    return m


def role(cmd):
    if 'GradleDaemon' in cmd: return 'daemon'
    e = re.search(r'Gradle Test Executor (\d+)', cmd)
    if e: return 'executor'
    if 'GradleWrapperMain' in cmd: return 'wrapper'
    if 'KotlinCompileDaemon' in cmd: return 'kotlin'
    if 'GradleWorkerMain' in cmd: return 'worker'
    return 'java'


def cgroup_info():
    info = {}
    try:
        rel = open('/proc/self/cgroup').read().strip().split('\n')[-1].split(':', 2)[2]
        info['cgroup'] = rel
        for d in ('/sys/fs/cgroup' + rel, '/sys/fs/cgroup'):
            for f in ('memory.max', 'memory.high', 'memory.peak', 'memory.current', 'memory.events', 'memory.swap.max'):
                p = os.path.join(d, f)
                if os.path.exists(p):
                    info[d + ':' + f] = open(p).read().strip().replace('\n', '; ')
    except Exception as e:
        info['error'] = str(e)
    return info


def sample():
    procs = []
    for pid in os.listdir('/proc'):
        if not pid.isdigit(): continue
        try:
            cmd = open(f'/proc/{pid}/cmdline', 'rb').read().replace(b'\0', b' ').decode('utf8', 'replace')
            if 'java' not in cmd.split(' ', 1)[0]: continue
            rss = 0
            for l in open(f'/proc/{pid}/status'):
                if l.startswith('VmRSS:'): rss = int(l.split()[1]) // 1024
            e = re.search(r'Gradle Test Executor (\d+)', cmd)
            procs.append([role(cmd), int(pid), rss, int(e.group(1)) if e else None])
        except Exception:
            pass
    m = meminfo()
    return {'t': int(time.time()), 'used': m['MemTotal'] - m['MemAvailable'], 'avail': m['MemAvailable'],
            'swap_used': m.get('SwapTotal', 0) - m.get('SwapFree', 0), 'procs': procs}


def start():
    if os.fork(): return
    os.setsid()
    if os.fork(): os._exit(0)
    open(PID, 'w').write(str(os.getpid()))
    subprocess.run(['sudo', 'sh', '-c', f'echo -1000 > /proc/{os.getpid()}/oom_score_adj'], capture_output=True)
    m = meminfo()
    with open(OUT, 'a') as f:
        f.write(json.dumps({'header': True, 't': int(time.time()), 'nproc': os.cpu_count(), 'mem_total': m['MemTotal'],
                            'machine': os.environ.get('SEMAPHORE_AGENT_MACHINE_TYPE'), 'cgroup': cgroup_info()}) + '\n')
    while True:
        try:
            with open(OUT, 'a') as f: f.write(json.dumps(sample()) + '\n')
        except Exception:
            pass
        time.sleep(INTERVAL)


def report():
    print('===== MEM-SAMPLER REPORT =====')
    print('cgroup at end:', json.dumps(cgroup_info()))
    k = subprocess.run(['sudo', 'dmesg', '-T'], capture_output=True, text=True).stdout
    oom = [l for l in k.splitlines() if re.search(r'out of memory|oom-kill|killed process|oom_reaper|invoked oom-killer', l, re.I)]
    print(f'kernel OOM lines: {len(oom)}')
    for l in oom[-40:]: print('  KERNEL', l)
    rows = [json.loads(l) for l in open(OUT)] if os.path.exists(OUT) else []
    hdr = next((r for r in rows if r.get('header')), {})
    rows = [r for r in rows if not r.get('header')]
    print('header:', json.dumps(hdr))
    if not rows:
        print('no samples'); return
    t0 = rows[0]['t']
    peak = {}
    last_min = None
    print('MIN used_mb avail_mb swap_mb n_exec exec_rss_sum_mb exec_rss_max_mb daemon_mb other_java_mb')
    for r in rows:
        ex = [p for p in r['procs'] if p[0] == 'executor']
        for p in r['procs']: peak[p[0]] = max(peak.get(p[0], 0), p[2])
        mnt = (r['t'] - t0) // 60
        if mnt != last_min:
            last_min = mnt
            dm = sum(p[2] for p in r['procs'] if p[0] == 'daemon')
            oth = sum(p[2] for p in r['procs'] if p[0] not in ('executor', 'daemon'))
            print('MEMSAMPLE', mnt, r['used'], r['avail'], r['swap_used'], len(ex), sum(p[2] for p in ex),
                  max((p[2] for p in ex), default=0), dm, oth)
    print('peak used_mb', max(r['used'] for r in rows), 'min avail_mb', min(r['avail'] for r in rows),
          'max concurrent executors', max(sum(1 for p in r['procs'] if p[0] == 'executor') for r in rows),
          'peak RSS by role', json.dumps(peak))
    lr = rows[-1]
    print('last sample', lr['t'] - t0, 's:', json.dumps(sorted(lr['procs'], key=lambda p: -p[2])[:25]))
    subprocess.run(['artifact', 'push', 'job', OUT, '--destination', 'mem-sampler.jsonl'], capture_output=True)


if __name__ == '__main__':
    {'start': start, 'report': report}[sys.argv[1]]()
