#!/usr/bin/env bash

#
# map-reduce tests
#

# comment this out to run the tests without the Go race detector.
RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]; then
  if go version | grep 'go1.17.[012345]'; then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' $(go version)
  fi
fi

TIMEOUT=timeout
if timeout 2s sleep 1 >/dev/null 2>&1; then
  :
else
  if gtimeout 2s sleep 1 >/dev/null 2>&1; then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]; then
  TIMEOUT+=" -k 2s 180s "
fi

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################

j=10
for ((i = 1; i <= j; i++)); do
  ../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
  sort mr-out-0 >mr-correct-wc.txt
  rm -f mr-out*
  start_time=$(date +%s)
  echo '***' Starting wc of "$i" worker test.

  $TIMEOUT ../mrcoordinator ../pg*txt &
  pid=$!

  # give the coordinator time to create the sockets.
  sleep 1

  # start multiple workers.
  # shellcheck disable=SC2004
  for ((count = 1; count <= ${j}; i++)); do
    $TIMEOUT ../mrworker ../../mrapps/wc.so
  done
  # wait for the coordinator to exit.
  wait $pid

  # since workers are required to exit when a job is completely finished,
  # and not before, that means the job has finished.
  sort mr-out* | grep . >mr-wc-all
  if cmp mr-wc-all mr-correct-wc.txt; then
    echo '---' wc of single worker test: PASS
  else
    echo '---' wc of single worker output is not the same as mr-correct-wc.txt
    echo '---' wc of single worker test: FAIL
    failed_any=1
    exit
  fi
  end_time=$(date +%s)
  cost_time=$(($end_time - $start_time))
  echo "test time is $(($cost_time / 60))min $(($cost_time % 60))s"
  er
  # wait for remaining workers and coordinator to exit.
  wait

  #########################################################
  # now indexer
  rm -f mr-*

done
