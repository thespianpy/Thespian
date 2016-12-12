typedef TXIntent {
  bit done;
};

int numPendingTransmits;

proctype asyncTX(TXIntent tx; chan res)
{
  tx.done = true;
  res ! tx
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
  }
  run actor()
}