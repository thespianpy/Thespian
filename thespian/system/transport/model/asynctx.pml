/*
 * $ spin asynctx.pml
 * $ spin -a asynctx.pml
 * $ gcc -o asynctx pan.c
 * $ asynctx -i
 * $ spin -t asynctx.pml
 */

typedef TXIntent {
  bit done;
};

int numPendingTransmits;
bit running_tx_queued;
bit lock;
bit processing;
chan pending = [5] of {TXIntent}; /* _aTB_queuedPendingTransmits */

inline interrupt_wait ()
{
  printf("interrupt_wait: TBD");
}

inline canSendNow (result)
{
  result = ! processing;
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

inline submitTransmit(tx, donechan)
{
  /* Simple for now */
  if
  :: true -> skip;
  :: true -> tx.done = true;
  fi
  donechan ! tx;
}

inline runQueued (result, donechan)
{
  bit havequeued, didexpired;
  TXIntent nextTX;
  complete_expired_intents(havequeued, didexpired);
  do
  :: didexpired -> complete_expired_intents(havequeued, didexpired);
  :: !didexpired -> break;
  od
  if
  :: havequeued ->
        get_lock();
        pending ? nextTX;
        lock = 0;
        submitTransmit(nextTX, donechan);
        result = true;
  :: !havequeued -> result = false;
  fi
}


proctype asyncTX(TXIntent tx; chan res)
{
  /* scheduleTransmit */
  /* _schedulePreparedIntent */
  bit csn, excl;
  pending ! tx;
  canSendNow(csn);
  if
  :: !csn ->
        if
        :: processing -> skip;
        :: !processing ->
              exclusively_processing(excl);
              if
              :: excl -> /* run drain_if_needed(delay); */
                         processing = false;
              :: !excl -> skip
              fi;
        fi
  :: csn ->
        exclusively_processing(excl);
        if
        :: !excl -> skip;
        :: excl ->
            bit r;
            do
            :: true ->
                  runQueued(r, res);
                  if
                  :: r -> skip;
                  :: !r ->
                        if
                        :: _pid == 0 -> skip;
                        :: _pid > 1 -> interrupt_wait();
                        fi
                        break;
                  fi;
            od;
            processing = false;
        fi;
  fi
}


proctype actor()
{
  chan result = [5] of { TXIntent };
  TXIntent t1, t2, t3, t4, t;
  d_step {
    t1.done = false;
    t2.done = false;
    t3.done = false;
    t4.done = false;
  };

  run asyncTX(t1, result);
  run asyncTX(t2, result);
  run asyncTX(t3, result);
  run asyncTX(t4, result);

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;

  result ? t;
  t.done == true;
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