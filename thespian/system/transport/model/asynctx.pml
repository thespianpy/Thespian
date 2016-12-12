/* This model validates the transmit functionality when there are
 * multiple threads in the current actor transmitting.
 *
 * $ spin asynctx.pml
 * $ spin -a asynctx.pml
 * $ gcc -DBITSTATE -o asynctx pan.c
 * $ asynctx -i
 * $ spin -p -t asynctx.pml
 *   or
 * $ ispin asynctx.pml
 */

typedef TXIntent {
  bit done;
  bit internal_update;
};

int numPendingTransmits;
bit lock;
bit processing;
bit has_interrupted;
chan pending = [5] of {TXIntent}; /* _aTB_queuedPendingTransmits */
int active_tx_pid;

inline canSendNow (result)
{
  // result = true;   /* Ignore numPendingTransmits for now */
  result = numPendingTransmits < 2;  // MAX_PENDING_TRANSMITS=2
}

inline get_lock ()
{
  d_step { lock == 0; lock = 1; }
}

inline exclusively_processing (result)
{
  get_lock();
  d_step {
    if
    :: processing -> result = false;
    :: !processing -> processing = true; result = true;
    fi;
  };
  lock = 0;
}


inline complete_expired_intents(ret_queued, ret_expired)
{
  TXIntent ptx;
  get_lock();
  d_step {
    /* For now, do not deal with expired intents */
    ret_expired = false;
    ret_queued = nempty(pending);
  };
  lock = 0;
}

inline runQueued (result, donechan, p_mainthread)
{
  printf("runQueued\n");
  bit havequeued, didexpired;
  TXIntent nextTX;
  complete_expired_intents(havequeued, didexpired);
  do
  :: didexpired -> complete_expired_intents(havequeued, didexpired);
  :: !didexpired -> break;
  od
  get_lock();
  if
  :: processing ->
        result = false;
        lock = 0;
  :: ! processing ->
        processing = true;
        if
        :: empty(pending) ->
              result = false;
              processing = false;
              lock = 0;
        :: nempty(pending) ->
              pending ? nextTX;
              lock = 0;
              numPendingTransmits++;
              printf("TX\n")
              d_step {
                assert(active_tx_pid == 0 || active_tx_pid == _pid);
                active_tx_pid = _pid;
              }
              run submitTransmit(nextTX, donechan, p_mainthread);
              get_lock();
              d_step {
                active_tx_pid = 0;
                processing = false;
              }
              lock = 0;
              result = true;
        fi;
  fi;
}

proctype submitTransmit(TXIntent tx; chan donechan; bit p_main)
{
  /* Transmit activity occurs here */
  printf("TX done\n");
  numPendingTransmits--;
  bit dcsn, dr;
  canSendNow(dcsn);
  do
  :: dcsn ->
        runQueued(dr, donechan, p_main);
        if
        :: dr -> skip;
        :: ! dr -> break;
        fi
  :: ! dcsn -> break;
  od
  /* Primary callback, sets tx intent to done */
  tx.done = true;
  donechan ! tx;
}

inline interrupt_wait (int_chan)
{
  TXIntent interrupt;
  interrupt.internal_update = true
  int_chan ! interrupt;
}

proctype asyncTX(chan tx_in; chan select_chan; chan res; bit is_main_thread)
{
  TXIntent tx;
  tx_in ? tx;
  do
  :: true ->
        /* scheduleTransmit */
        /* _schedulePreparedIntent */
        bit csn, excl;
        if
        :: tx.internal_update ->
              has_interrupted = false;
        :: ! tx.internal_update ->
              get_lock();
              pending ! tx;
              lock = 0;
        fi
        canSendNow(csn);
        if
        :: !csn ->
              printf("Blocked\n");
              exclusively_processing(excl);
              if
              :: excl -> /* run drain_if_needed(delay); */
                         printf("Draining\n");
                         processing = false;
              :: !excl -> skip
              fi
        :: csn -> skip;
        fi
        printf("Sending all\n");
        canSendNow(csn);
        do
        :: csn ->
              bit r;
              runQueued(r, res, is_main_thread);
              if
              :: r -> skip;
              :: !r ->
                    if
                    :: is_main_thread -> skip;
                    :: has_interrupted -> skip;
                    :: ! is_main_thread && ! has_interrupted ->
                          printf("Tell main to grab work\n");
                          has_interrupted = true;
                          interrupt_wait(select_chan);
                    fi
                    break;
              fi;
              canSendNow(csn);
        :: !csn -> break;
        od;
        printf("MSC: Waiting for more TX\n");
end_idle_select:       tx_in ? tx;  /* select waits for new stuff */
  od;
}


proctype actor()
{
  chan result = [10] of { TXIntent };
  chan main_thread_tx = [5] of { TXIntent };
  chan thrd1_tx = [5] of { TXIntent };
  chan thrd2_tx = [5] of { TXIntent };
  TXIntent t1, t2, t3, t4, t5, t6, t;
  d_step {
    t1.done = false; t1.internal_update = false;
    t2.done = false; t2.internal_update = false;
    t3.done = false; t3.internal_update = false;
    t4.done = false; t4.internal_update = false;
    t5.done = false; t5.internal_update = false;
  };

  run asyncTX(main_thread_tx, main_thread_tx, result, true);
  run asyncTX(thrd1_tx, main_thread_tx, result, false);
  run asyncTX(thrd2_tx, main_thread_tx, result, false);

  main_thread_tx ! t1;
  thrd1_tx ! t2;
  thrd2_tx ! t3;
  thrd2_tx ! t4;
  thrd2_tx ! t5;
  thrd2_tx ! t6;

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;


  empty(pending);  // No more pending work
  empty(main_thread_tx);  // No more work in progress, including interrupt waits
  empty(result);  // No more results
}

init
{
  d_step {
    numPendingTransmits = 0;
    lock = false;
    processing = false;
    active_tx_pid = 0;
    has_interrupted = false;
  }
  run actor()
}