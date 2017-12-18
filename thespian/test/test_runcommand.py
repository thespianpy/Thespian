import sys
import datetime
from thespian.test import *
from thespian.actors import *
import thespian.runcommand

ask_timeout = datetime.timedelta(seconds=7)



class TestRunCommand(object):
    def testCreateActorSystem(self, asys):
        pass

    def testSimpleCommand(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-c', 'print("hello")']),
                       ask_timeout)
        print(res)
        assert res
        assert res.stdout == 'hello\n'


    def testSimpleTaggedCommand(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-c', 'print("hello")'],
                                                        logtag='hi'),
                       ask_timeout)
        print(res)
        assert res
        assert res.stdout == 'hello\n'


    def testNonExistentCommand(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable[3:-3], ['-c', 'print("hello")'],
                                                        logtag='hi'),
                       ask_timeout)
        print(res)
        assert not res
        assert res.stderr
        assert 'FAILED' in str(res)


    def testSlowCommandOutputOnly(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = '\n'.join(['import time',
                             'print("hello")',
                             'time.sleep(0.5)',
                             'print("world")',
                             'time.sleep(0.5)',
        ])
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-u', '-c', program],
                                                        logtag='hi',
                                                        report_on_start=True),
                       ask_timeout)
        print(res)
        assert isinstance(res, thespian.runcommand.CommandStarted)
        assert res.pid >= 1
        assert res.command.logtag == 'hi'
        res = asys.listen(ask_timeout)
        assert res
        assert res.stdout == 'hello\nworld\n'

    def testSlowCommandTimeout(self, asys):
        #actor_system_unsupported(asys, 'simpleSystemBase') #, 'multiprocQueueBase')
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = '\n'.join(['import time',
                             'print("hello")',
                             'time.sleep(2.5)',
                             'print("world")',
                             'time.sleep(0.5)',
        ])
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-u', '-c', program],
                                                        logtag='hi',
                                                        timeout=2),
                       ask_timeout)
        print(res)
        assert not res

    def testSlowCommandTimeDelta(self, asys):
        #actor_system_unsupported(asys, 'simpleSystemBase') #, 'multiprocQueueBase')
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = '\n'.join(['import time',
                             'print("hello")',
                             'time.sleep(2.5)',
                             'print("world")',
                             'time.sleep(0.5)',
        ])
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-u', '-c', program],
                                                        logtag='hi',
                                                        timeout=datetime.timedelta(seconds=2)),
                       ask_timeout)
        print(res)
        assert not res

    def testSlowCommandWantingInputNoneAvailable(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = '\n'.join(['import time',
                             'print("hello")',
                             'time.sleep(0.5)',
                             'import sys',
                             'sys.stdout.write("Who are you? ")',
                             'r = sys.stdin.read()',
                             'print("\\nhello %s" % r)',
                             'time.sleep(0.5)',
        ])
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-c', program],
                                                        logtag='hi'),
                       ask_timeout)
        print(res)
        assert res
        assert res.stdout == 'hello\nWho are you? \nhello \n'

    def testSlowCommandWantingInputAvailable(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = '\n'.join(['import time',
                             'print("hello")',
                             'time.sleep(0.5)',
                             'import sys',
                             'sys.stdout.write("Who are you? ")',
                             'sys.stdout.flush()',
                             'r = sys.stdin.read()',
                             'print("\\nhello %s" % r)',
                             'time.sleep(0.5)',
        ])
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-c', program],
                                                        logtag='hi',
                                                        input_src='Harry\n'
        ),
                       ask_timeout)
        print(res)
        assert res
        assert res.stdout == 'hello\nWho are you? \nhello Harry\n\n'

    def testSlowShellCommandWantingInputAvailable(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = ';'.join(['echo howdy',
                            'sleep 1',
                            'echo -n Who are you?',
                            'read r',
                            'echo',
                            'echo hello $r',
                            'sleep 1',
        ])
        res = asys.ask(cmd, thespian.runcommand.Command('bash', ['-c', program],
                                                        logtag='yo',
                                                        input_src='Harry\n'
        ),
                       ask_timeout)
        print(res)
        assert res
        assert res.stdout == 'howdy\nWho are you?\nhello Harry\n'

    def testWatchedSlowCommandWantingInputAvailable(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = '\n'.join(['import time',
                             'print("hello")',
                             'time.sleep(0.5)',
                             'import sys',
                             'sys.stdout.write("Who are you? ")',
                             'sys.stdout.flush()',
                             'r = sys.stdin.read()',
                             'print("\\nhello %s" % r)',
                             'sys.stderr.write("All done\\n")',
                             'sys.stderr.flush()',
                             'time.sleep(0.5)',
        ])
        watcher = asys.createActor(Watcher)
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-c', program],
                                                        logtag='hi',
                                                        input_src='Harry\n',
                                                        output_updates=watcher
        ),
                       ask_timeout)
        print(res)
        assert res
        print(res.stdout)
        assert res.stdout == 'hello\nWho are you? \nhello Harry\n\n'
        assert res.stderr == 'All done\n'
        watched = asys.ask(watcher, 1, ask_timeout)
        print(res.stdout)
        print('--')
        try:
            watched_out = ''.join(watched[0])
            watched_err = ''.join(watched[1])
        except TypeError:
            watched_out = ''.join([bs.decode('utf-8') for bs in watched[0]])
            watched_err = ''.join([bs.decode('utf-8') for bs in watched[1]])
        print(watched_out)
        print('==')
        print(res.stderr)
        print('--')
        print(watched_err)
        assert (res.stdout, res.stderr) == (watched_out, watched_err)

    def testWatchedSlowShellCommandWantingInputAvailable(self, asys):
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = ';'.join(['echo howdy',
                            'sleep 1',
                            'echo -n Who are you?',
                            'read r',
                            'echo',
                            'echo hello $r',
                            'echo all done >&2',
                            'sleep 1',
        ])
        watcher = asys.createActor(Watcher)
        res = asys.ask(cmd, thespian.runcommand.Command('bash', ['-c', program],
                                                        logtag='yo',
                                                        input_src='Harry\n',
                                                        output_updates=watcher
        ),
                       ask_timeout)
        print(res)
        assert res
        assert res.stdout == 'howdy\nWho are you?\nhello Harry\n'
        assert res.stderr == 'all done\n'
        watched = asys.ask(watcher, 1, ask_timeout)
        print(res.stdout)
        print('--')
        try:
            watched_out = ''.join(watched[0])
            watched_err = ''.join(watched[1])
        except TypeError:
            watched_out = ''.join([bs.decode('utf-8') for bs in watched[0]])
            watched_err = ''.join([bs.decode('utf-8') for bs in watched[1]])
        print(watched_out)
        print('==')
        print(res.stderr)
        print('--')
        print(watched_err)
        assert (res.stdout, res.stderr) == (watched_out, watched_err)

    def testCommandAbort(self, asys):
        # Only run this for system bases supporting Thespian Watch
        actor_system_unsupported(asys, 'simpleSystemBase',
                                 'multiprocQueueBase',
                                 'multiprocUDPBase')
        cmd = asys.createActor(thespian.runcommand.RunCommand)
        program = '\n'.join(['print("hello")',
                             'import time',
                             'time.sleep(60)',
                             'print("\\ngoodbye")',
        ])
        res = asys.ask(cmd, thespian.runcommand.Command(sys.executable, ['-c', program],
                                                        logtag='hi'),
                       datetime.timedelta(seconds=2))
        print(res)
        assert res is None
        res2 = asys.ask(cmd, thespian.runcommand.CommandAbort(),
                        datetime.timedelta(seconds=5))
        assert res2.exitcode == -15  # corresponds to SIGTERM
        assert res2.stdout == ''
        assert res2.stderr == ''
        assert res2.duration < datetime.timedelta(seconds=7)


class Watcher(ActorTypeDispatcher):
    def __init__(self, *args, **kw):
        super(Watcher, self).__init__(*args, **kw)
        self.output = []
        self.errors = []
    def receiveMsg_CommandOutput(self, outmsg, sender):
        self.output.append(outmsg.output)
    def receiveMsg_CommandError(self, errmsg, sender):
        self.errors.append(errmsg.error_output)
    def receiveMsg_int(self, intmsg, sender):
        self.send(sender, (self.output, self.errors))
