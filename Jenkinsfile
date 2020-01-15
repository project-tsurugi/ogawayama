pipeline {
    agent {
        docker {
            image 'project-tsurugi/oltp-sandbox'
            label 'docker'
        }
    }
    environment {
        GITHUB_URL = 'https://github.com/project-tsurugi/ogawayama'
        GITHUB_CHECKS = 'tsurugi-check'
        BUILD_PARALLEL_NUM="""${sh(
                returnStdout: true,
                script: 'grep processor /proc/cpuinfo | wc -l'
            )}"""
        INSTALL_PREFIX="$WORKSPACE/.local"
    }
    stages {
        stage ('Prepare env') {
            steps {
                sh '''
                    ssh-keyscan -t rsa github.com > ~/.ssh/known_hosts
                '''
            }
        }
        stage ('checkout master') {
            steps {
                checkout scm
                sh '''
                    # git clean -dfx
                    git submodule sync --recursive
                    git submodule update --init --recursive
                '''
            }
        }
        stage ('Install umikongo') {
            steps {
                sh '''
                    rm -fr ${INSTALL_PREFIX}
                    mkdir ${INSTALL_PREFIX}

                    # install shakujo
                    cd ${WORKSPACE}/third_party/umikongo/third_party/shakujo
                    mkdir -p build
                    cd build
                    rm -f CMakeCache.txt
                    cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DFORCE_INSTALL_RPATH=ON -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
                    make -j${BUILD_PARALLEL_NUM}
                    make install

                    # install kvs_charkey
                    cd ${WORKSPACE}/third_party/umikongo/third_party/kvs_charkey
                    git log -n 1 --format=%H
                    ./bootstrap.sh
                    mkdir -p build
                    cd build
                    cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON -DENABLE_SANITIZER=ON -DCMAKE_INSTALL_PREFIX=${WORKSPACE}/.local ..
                    make clean
                    make all install -j${BUILD_PARALLEL_NUM}

                    # install sharksfin
                    cd ${WORKSPACE}/third_party/umikongo/third_party/sharksfin
                    mkdir -p build
                    cd build
                    rm -f CMakeCache.txt
                    cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DFORCE_INSTALL_RPATH=ON -DBUILD_FOEDUS_BRIDGE=OFF -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
                    make -j${BUILD_PARALLEL_NUM}
                    make install

                    # install umikongo
                    cd ${WORKSPACE}/third_party/umikongo
                    mkdir -p build
                    cd build
                    rm -f CMakeCache.txt
                    cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DFORCE_INSTALL_RPATH=ON -DFIXED_PAYLOAD_SIZE=ON -DSHARKSFIN_IMPLEMENTATION=memory -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
                    make -j${BUILD_PARALLEL_NUM}
                    make install
                '''
            }
        }
        stage ('Build') {
            steps {
                sh '''
                    mkdir -p build
                    cd build
                    cmake -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON -DCMAKE_PREFIX_PATH=${WORKSPACE}/.local -DCMAKE_EXPORT_COMPILE_COMMANDS=ON ..
                    make all -j${BUILD_PARALLEL_NUM}
                '''
            }
        }
        stage ('Test') {
            environment {
                GTEST_OUTPUT="xml"
            }
            steps {
                sh '''
                    cd build
                    make test ARGS="--verbose"
                '''
            }
        }
        // stage ('Doc') {
        //     steps {
        //         sh '''
        //             cd build
        //             make doxygen > doxygen.log 2>&1
        //             zip -q -r ogawayama-doxygen doxygen/html
        //         '''
        //     }
        // }
        // stage ('Coverage') {
        //     environment {
        //         GCOVR_COMMON_OPTION='-e ../third_party/ -e ../.*/test.* -e ../.*/examples.* -e ../.local/.*'
        //         BUILD_PARALLEL_NUM=4
        //     }
        //     steps {
        //         sh '''
        //             cd build
        //             mkdir gcovr-xml gcovr-html
        //             gcovr -j ${BUILD_PARALLEL_NUM} -r .. --xml ${GCOVR_COMMON_OPTION} -o gcovr-xml/ogawayama-gcovr.xml
        //             gcovr -j ${BUILD_PARALLEL_NUM} -r .. --html --html-details --html-title "ogawayama coverage" ${GCOVR_COMMON_OPTION} -o gcovr-html/ogawayama-gcovr.html
        //             zip -q -r ogawayama-coverage-report gcovr-html
        //         '''
        //     }
        // }
        stage ('Lint') {
            steps {
                sh '''#!/bin/bash
                    python tools/bin/run-clang-tidy.py -quiet -export-fixes=build/clang-tidy-fix.yaml -p build -extra-arg=-Wno-unknown-warning-option -header-filter=$(pwd)'/(common|server|stub)/.*\\.h$' $(pwd)'/(common|server|stub)/.*\\.cpp$' > build/clang-tidy.log 2> build/clang-tidy-error.log
                '''
            }
        }
        // stage ('Graph') {
        //     steps {
        //         sh '''
        //             cd build
        //             cp ../cmake/CMakeGraphVizOptions.cmake .
        //             cmake --graphviz=dependency-graph/umikongo.dot ..
        //             cd dependency-graph
        //             dot -T png ogawayama.dot -o umikongo-tpcc.png
        //         '''
        //     }
        // }
    }
    post {
        always {
            xunit tools: ([GoogleTest(pattern: '**/*_gtest_result.xml', deleteOutputFiles: false, failIfNotNew: false, skipNoTestFiles: true, stopProcessingIfError: true)]), reduceLog: false
            recordIssues filters: [excludeFile('/usr/include/*')], tool: clangTidy(pattern: 'build/clang-tidy.log'),
                qualityGates: [[threshold: 1, type: 'TOTAL', unstable: true]]
            recordIssues tool: gcc4(),
                enabledForFailure: true
            // recordIssues tool: doxygen(pattern: 'build/doxygen.log')
            recordIssues tool: taskScanner(
                highTags: 'FIXME', normalTags: 'TODO',
                includePattern: '**/*.md,**/*.txt,**/*.in,**/*.cmake,**/*.cpp,**/*.h',
                excludePattern: 'third_party/**,build/**,.local/**'),
                enabledForFailure: true
            // publishCoverage adapters: [coberturaAdapter('build/gcovr-xml/ogawayama-gcovr.xml')], sourceFileResolver: sourceFiles('STORE_ALL_BUILD')
            archiveArtifacts allowEmptyArchive: true, artifacts: 'build/ogawayama-coverage-report.zip, build/clang-tidy.log, build/clang-tidy-fix.yaml', onlyIfSuccessful: true
            notifySlack('tsurugi-dev')
        }
    }
}
