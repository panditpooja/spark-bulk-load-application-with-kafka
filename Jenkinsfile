pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
               sh '/usr/local/bin/pipenv --python python3 sync'
            }
        }
        stage('Test') {
            steps {
               sh '/usr/local/bin/pipenv run pytest'
            }
        }
        stage('Package') {
        when{
           anyOf{ branch "main" ; branch 'release' }
        }
            steps {
               sh 'zip -r sbdl.zip lib'
            }
        }
    stage('Release') {
       when{
          branch 'release'
       }
           steps {
              sh 'cp sbdl.zip /home/pipandit170/sbdl-qa/'
           }
        }
    stage('Deploy') {
       when{
          branch 'main'
       }
           steps {
               sh 'cp sbdl.zip /home/pipandit170/sbdl-prod/'
           }
        }
    }
}