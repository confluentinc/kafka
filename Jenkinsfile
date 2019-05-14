#!/usr/bin/env groovy

def config = jobConfig {
    cron = '@midnight'
    nodeLabel = 'docker-oraclejdk8'
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    //slackChannel = '#kafka'
    slackChannel = '#things'
    timeoutHours = 4
}


def job = {
    // Per KAFKA-7524, Scala 2.12 is the default, yet we currently support the previous minor version.
    stage("Check compilation compatibility with Scala 2.11") {
        sh "gradle"
        sh "./gradlew clean compileJava compileScala compileTestJava compileTestScala " +
                "--no-daemon --stacktrace -PscalaVersion=2.11"
    }

    stage("Compile and validate") {
        sh "./gradlew clean compileJava compileScala compileTestJava compileTestScala " +
                "spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain " +
                "--no-daemon --stacktrace --continue -PxmlSpotBugsReport=true"
    }

    stage("Test") {
        sh "./gradlew unitTest integrationTest " +
                "--no-daemon --stacktrace --continue -PtestLoggingEvents=started,passed,skipped,failed -PmaxParallelForks=4"
    }

    /*
    def kafkaRepo = sh(script: 'git config --get remote.origin.url', returnStdout: true).substring('https://github.com/'.size()).trim();
    def kafkaBranch = env.BRANCH_NAME;
    def muckrakeBranch = "master";
    // Start the downstream job.
    stage("Trigger test-cp-downstream-builds-kafka") {
        echo "Schedule test-cp-downstream-builds-kafka with muckrake branch ${muckrakeBranch} and Apache Kafka branch ${kafkaRepo}:${kafkaBranch}."
        buildResult = build job: 'test-cp-downstream-builds-kafka', parameters: [
                [$class: 'StringParameterValue', name: 'BRANCH', value: muckrakeBranch],
                [$class: 'StringParameterValue', name: 'KAFKA_REPO', value: kafkaRepo],
                [$class: 'StringParameterValue', name: 'KAFKA_BRANCH', value: kafkaBranch],
                [$class: 'StringParameterValue', name: 'NODE_LABEL', value: "docker-oraclejdk8"]],
                propagate: false, wait: false
    }
    */

    configFileProvider([configFile(fileId: 'Gradle Nexus Settings', variable: 'GRADLE_NEXUS_SETTINGS')]) {
        stage("Update the Confluent Kafka repository") {
            sh "./gradlew --init-script ${GRADLE_NEXUS_SETTINGS} uploadArchivesAll"
        }
    }
}

def post = {
    stage('Upload results') {
        // Kafka failed test stdout files
        archiveArtifacts artifacts: '**/testOutput/*.stdout', allowEmptyArchive: true

        def summary = junit '**/build/test-results/**/TEST-*.xml'
        def total = summary.getTotalCount()
        def failed = summary.getFailCount()
        def skipped = summary.getSkipCount()
        summary = "Test results:\n\t"
        summary = summary + ("Passed: " + (total - failed - skipped))
        summary = summary + (", Failed: " + failed)
        summary = summary + (", Skipped: " + skipped)
        return summary;
    }
}

runJob config, job, post
