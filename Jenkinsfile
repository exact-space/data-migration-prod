def deploymentStatus = [:]
def deploymentStatusList = []

pipeline {
    agent any  
    environment {
        VERSION = "${env.BUILD_ID}"
        registry = "dev.exactspace.co"
        APP_NAME = "data-migration-prod-es"
    }
    stages {
        stage("get scm") {
            steps {
                checkout([$class: 'GitSCM',
                    branches: [[name: 'main']],
                    userRemoteConfigs: [[url: 'git@github.com:exact-space/data-migration-prod.git']]
                ])
            }
        }
        stage("cython compilation") {
            steps {
                sh "cython index.py --embed"
            }
        }
        stage("building images") {
            steps {
                sh "sudo docker build --rm --no-cache -t $APP_NAME:r1 ."
            }
        }
        stage("tagging images-r1") {
            steps {
                sh "sudo docker tag $APP_NAME:r1 $registry/$APP_NAME:r1"
            }
        }
        stage("remove old docker image-r1") {
            steps {
                sh "sudo docker image rm $APP_NAME:r1"
            }
        }
        stage("image push-r1") {
            steps {
                sh "sudo docker push $registry/$APP_NAME:r1"
            }
        }
        stage('deploying to Prod') {
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def status = deploy("https://data.exactspace.co/deployservice/cicd/stack/$APP_NAME", 'Prod')
                        status.name = 'Prod'
                        deploymentStatus['Prod'] = status ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << status
                    }
                }
            }
        }
       /* stage('deploying to UTCL') {
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def status = deploy("https://cpp.utclconnect.com/deployservice/cicd/stack/$APP_NAME", 'UTCL')
                        status.name = 'UTCL'
                        deploymentStatus['UTCL'] = status ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << status
                    }
                }
            }
        }
        stage('deploying to HRD') {
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def status = deploy("http://40.88.150.243:9001/hrd/deployservice/cicd/stack/$APP_NAME", 'HRD')
                        status.name = 'HRD'
                        deploymentStatus['HRD'] = status ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << status
                    }
                }
            }
        }
        stage('deploying to LPG') {
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def status = deploy("http://40.88.150.243:9001/lpg/deployservice/cicd/stack/$APP_NAME", 'LPG')
                        status.name = 'LPG'
                        deploymentStatus['LPG'] = status ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << status
                    }
                }
            }
        }
        stage('deploying to BHEL') {
            steps {
                script {
                    catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                        def status = deploy("https://rmds.bhel.in/deployservice/cicd/stack/$APP_NAME", 'BHEL')
                        status.name = 'BHEL'
                        deploymentStatus['BHEL'] = status ?: [result: 'FAILURE', message: 'Deployment failed']
                        deploymentStatusList << status
                    }
                }
            }
        }*/

    }
    post {
        always {
            script {
                def buildStartTime = new Date(currentBuild.rawBuild.getTimeInMillis())
                def buildEndTime = new Date(currentBuild.rawBuild.startTimeInMillis + currentBuild.duration)

                def emailAddress = readFile("${env.WORKSPACE}/mail.txt").trim().split("\\s*,\\s*").join(", ")
                if (emailAddress){
                    emailext body: createEmailBody(buildStartTime, buildEndTime, deploymentStatus),
                            subject: "Deployment Status for ${currentBuild.fullDisplayName}",
                            to: emailAddress
                }
            }
        }
    } 
}

def deploy(deploymentUrl, dataCentre) {
    def status = [:]

    timeout(time: 15, unit: 'MINUTES') {
        def response = httpRequest url: deploymentUrl, timeout: 900000
        def statusCode = response.status

        echo "${statusCode}"
        echo "${dataCentre}"

        // Check HTTP response code to determine success or failure
        if (statusCode == 200) {
            // Deployment successful
            status.result = 'SUCCESS'
            status.message = "Deployment success with response code ${statusCode},"
        } else {
            // Deployment failed
            status.result = 'FAILURE'
            status.message = "Deployment failed with response code ${statusCode},"
        }

        return status
    }
}

def createEmailBody(buildStartTime, buildEndTime, deploymentStatusList) {
    def body = "Build triggered at: ${buildStartTime}\n\nDeployment Status:\n"

    //def dataCenters = ['Prod', 'UTCL', 'HRD', 'LPG', 'BHEL']
    def dataCenters = ['sandbox']

    dataCenters.each { dataCentre ->
        body += "${dataCentre} : \n"
        def status = deploymentStatusList.find { it.key == dataCentre }?.value ?: [:]

        if (status) {
            if (status['result'] == "SUCCESS") {
                body += "${status['message']},\n"
            } else if (status['result'] == "FAILURE") {
                body += "${status['message']},\n"
            }
        } else {
            // If status is not found, assume a default failed status for that data center
            body += "Deployment Failed , Please check full logs, \n\n"
        }
    }

    def buildUrl = currentBuild.absoluteUrl
    body += "\nJenkins Build Log: ${buildUrl}\n"
    body += "\nBuild ended at: ${buildEndTime}\n"

    return body
}    
