/*
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
bit running_tx_queued;
bit lock;
bit processing;
chan pending = [5] of {TXIntent}; /* _aTB_queuedPendingTransmits */

inline canSendNow (result)
{
  result = true;   /* Ignore numPendingTransmits for now */
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
              processing = false;
              lock = 0;
              numPendingTransmits = numPendingTransmits + 1;
              run submitTransmit(nextTX, donechan, p_mainthread);
              result = true;
        fi;
  fi;
}

proctype submitTransmit(TXIntent tx; chan donechan; bit p_main)
{
  /* Transmit activity occurs here */
  numPendingTransmits = numPendingTransmits - 1;
  if
  :: running_tx_queued -> skip;
  :: ! p_main -> skip;
  :: p_main && ! running_tx_queued ->
        running_tx_queued = true;
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
        running_tx_queued = false;
  fi;
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
        :: tx.internal_update -> skip
        :: ! tx.internal_update ->
              get_lock();
              pending ! tx;
              lock = 0;
        fi
        canSendNow(csn);
        if
        :: !csn ->
              exclusively_processing(excl);
              if
              :: excl -> /* run drain_if_needed(delay); */
                         processing = false;
              :: !excl -> skip
              fi
        :: csn ->
              bit r;
              do
              :: true ->
                    runQueued(r, res, is_main_thread);
                    if
                    :: r -> skip;
                    :: !r ->
                          if
                          :: is_main_thread -> skip;
                          :: ! is_main_thread ->
                                interrupt_wait(select_chan);
                          fi
                          break;
                    fi;
              od;
        fi;
end_idle_select:       tx_in ? tx;  /* select waits for new stuff */
  od;
}


proctype actor()
{
  chan result = [5] of { TXIntent };
  chan main_thread_tx = [1] of { TXIntent };
  chan thrd1_tx = [1] of { TXIntent };
  chan thrd2_tx = [1] of { TXIntent };
  chan thrd3_tx = [1] of { TXIntent };
  TXIntent t1, t2, t3, t4, t;
  d_step {
    t1.done = false; t1.internal_update = false;
    t2.done = false; t2.internal_update = false;
    t3.done = false; t3.internal_update = false;
    t4.done = false; t4.internal_update = false;
  };

  run asyncTX(main_thread_tx, main_thread_tx, result, true);
  run asyncTX(thrd1_tx, main_thread_tx, result, false);
  run asyncTX(thrd2_tx, main_thread_tx, result, false);

  main_thread_tx ! t1;
  thrd1_tx ! t2;
  thrd2_tx ! t3;
  thrd2_tx ! t4;

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
    running_tx_queued = false;
    lock = false;
    processing = false;
  }
  run actor()
}