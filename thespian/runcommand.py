"""This is a helper which provides a set of definitions and Actors
that can be used to run external commands and gather responses from
them.

Create a RunCommand Actor and send it a Command object defining the
command to be run; the RunCommand will execute the command, monitoring
its progress, and sends a CommandResult object when it completes.

If the current Thespian system base supports "Watch" functionality,
the RunCommand Actor will remain responsive while the command is
performed and can interact with an input_src Actor to provide ongoing
input.  If no "Watch" functionality is available, the RunCommand will
block waiting for the command to complete and can only provide static
input to the command.
"""

from datetime import datetime, timedelta
import fcntl,os
import logging
import subprocess
import time
from thespian.actors import *

HALF_NUM_LINES_LOGGED=5

class Command(object):
    """Defines the Command to be run by the RunCommand Actor.

       The 'exe' argument specifies the name of the executable as either
       an absolute path or a relative name to be found on the current
       PATH.  The 'args' is a list of arguments passed to the
       executable on the command line.

       The 'use_shell' defaults to False, but can be set to True to
       indicate that the command should be run via the normal shell.
       (This is normally not recommended.)

       The 'omit_string' argument can specify a string that should be
       suppressed from any logging output generated for this command
       (useful for suppressing sensitive information like passwords,
       credit card numbers, etc. that should not appear in logs).

       The 'error_ok' argument controls the logging mode if the
       command fails: normally errors will be logged with the ERROR
       severity, but if this argument is true, the INFO severity is
       used instead.

       The 'logger' specifies what logging should be done for the
       command, especially in error conditions.  The default is None,
       which uses the standard logging endpoint when starting the
       command and with the results of the command.  A value of False
       specifies that no logging is to be performed, otherwise it
       should be an ActorAddress which will receive CommandLog
       objects.

       The 'logtag' argument can be used to specify the prefix value
       for identifying all logged output from this command run; it
       defaults to the string form of the 'exe' argument, but this may
       be long, and also generic if multiple intances of the same
       command are run.

       The 'input_src' argument specifies any input that should be
       provided to the command.  The default is None, which indicates
       that no input will be provided to the command that is run.  If
       a string is specified, it is supplied as the stdin to the
       executable.

       The 'output_updates' argument is either None or specifies an
       ActorAddress to which output will be sent as it is generated.
       The 'omit_string' and 'max_bufsize' arguments do not affect
       this output, and output is not necessarily sent in complete
       lines.  Normal output is sent in CommandOutput messages and
       error output is sent in CommandError messages.  Note that for
       system bases which do not support the Thespian Watch
       functionality, the output will only be sent once the command
       completes.

       The 'max_bufsize' specifies the maximum amount of normal output
       or error output that will be collected.  If command output is
       in excess of this amount then the middle portion is dropped
       (the CommandResult output an error will be a tuple of the
       beginning and end parts of the output).  The default is 1MB of
       output (the limit is applied to normal output and error output
       separately, so the total amount of memory that can be consumed
       is double the max_bufsize amount).

       The 'env' argument specifies the environment varaibles that
       should be set for the new process; if not specified, the
       current environment is inherited.

       The 'timeout' argument should specify a maximum time period for
       the command to run to completion.  The default is None which
       does not set a time limit.  If specified, the value should be a
       datetime.timedelta, or an integer number of seconds; once the
       command run has exceeded that time limit, the command is halted
       with a SIGTERM, followed 2 seconds later with a SIGKILL.

       If 'report_on_start' is set to True, then the requestor will
       receive a CommandStarted message when the command process has
       been started.

    """
    def __init__(self, exe, args,
                 use_shell=False,
                 omit_string=None,
                 error_ok=False,
                 logger=None,
                 logtag=None,
                 input_src=None,
                 output_updates=None,
                 max_bufsize=1024*1024,
                 env=None,
                 timeout=None,
                 report_on_start=False):
        self.exe = exe
        self.args = args
        self.use_shell = use_shell
        self.omit_string = omit_string or ''
        self.error_ok = error_ok
        self.logger = logger
        self.logtag = logtag or str(exe)
        self.input_src = input_src
        self.output_updates = output_updates
        self.max_bufsize = max_bufsize
        self.env = env
        self.timeout = (timeout if isinstance(timeout, timedelta)
                        else timedelta(seconds=timeout)) if timeout else timeout
        self.report_on_start = report_on_start


class CommandStarted(object):
    """Message sent by the RunCommand actor to the sender of a Command to
       indicate that the command has been initiated when the
       Command.report_on_start is True.
    """
    def __init__(self, command, pid):
        self.command = command
        self.pid = pid


class CommandAbort(object):
    """Message sent to the RunCommand Actor to request a halt of the
       currently running command.  Note that this will only be
       processed asynchronously in a Thespian System Base that
       supports the ThespianWatch functionality.  If there is no
       command currently running, this message does nothing.  There is
       no response to this message (although it is expected that a
       CommandError will subsequently be generated when the running
       Command is aborted).
    """


class CommandLog(object):
    "Message sent by RunCommand to the specified logger address."
    def __init__(self, level, msg, *args):
        self.level = level  # a string: "info" or "error"
        self.message = msg % args


class CommandOutput(object):
    """Message specifying (possibly partial) output received from the
       command, and sent to the output_updates Actor.  This may be
       sent multiple times as output is generated by the running
       process.  The output is either a string (Python2) or a
       bytestring (Python3) as would normally be returned by a read()
       from a pipe.

    """
    def __init__(self, command, output):
        self.command = command  # the Command being run
        self.output = output


class CommandError(object):
    """Message specifying (possibly partial) error output received from
       the command.  This is normally sent to the output_updates Actor.
       This may be sent multiple times as output is generated by the
       running process.  The output is either a string (Python2) or a
       bytestring (Python3) as would normally be returned by a read()
       from a pipe.
    """
    def __init__(self, command, error_output):
        self.command = command  # the Command being run
        self.error_output = error_output


class CommandResult(object):
    """Describes the result of executing a Command.  Is "truthy" if the
       Command completed successfully.  Provides the normal and error
       output generated by the Command execution (unfiltered).  Sent
       by the RunCommand Actor back to the sender of the Command to
       indicate the completion state.

          .command = original Command message
          .exitcode = return code from executed command
          .stdout = normal output string from executed command
          .stderr = error output string from executed command
          .errorstr = stderr, or "FAILED" if no stderr and command failed
          .duration = timedelta indicating duration of command run

        The exitcode will be -2 if the command timed out.  Commands
        are run sequentially, and Command timing does not start until
        the command is run; Commands sent to the RunCommand actor
        while a previous command is running will be queued until the
        current command completes.

        The .stdout and .stderr values may be a tuple of two strings
        instead of a string in the case that the size of the
        corresponding output was in excess of the max_bufsize
        specified in the Command; in this case the first element of
        the tuple is the beginning of the output, the second element
        of the tuple is the end of the output, and the middle portion
        is missing.

    """
    def __init__(self, command, exitcode, stdout='', stderr='', duration=None):
        self.command = command  # original Command message
        self.exitcode = exitcode
        self.stdout = stdout
        self.stderr = stderr
        self.duration = duration

    def __nonzero__(self):
        return 1 if 0 == self.exitcode else 0  # Python2: truthy if command success (exitcode==0)

    def __bool__(self):
        return 0 == self.exitcode   # Python3: truthy if command success (exitcode==0)

    @property
    def errorstr(self):
        return self.stderr or ('' if self else 'FAILED')

    def __str__(self):
        rval = [self.__class__.__name__, 'success' if self else 'FAILED',]
        if not self:
            rval.extend(['Error #%d' % self.exitcode,
                         ' [...] '.join(self.stderr)
                         if isinstance(self.stderr, tuple) else
                         self.stderr])
        return ' '.join(rval)


def str_form(bytestr):
    try:
        return bytestr.decode('utf-8')
    except UnicodeDecodeError:
        try:
            import chardet
            try:
                return bytestr.decode(chardet.detect(bytestr)['encoding'])
            except UnicodeDecodeError:
                pass
        except ImportError:
            pass
    except AttributeError:
        return bytestr  # already a string
    outs = str(bytestr)



class RunCommand(ActorTypeDispatcher):

    def __init__(self, capabilities, *args, **kw):
        super(RunCommand, self).__init__(*args, **kw)
        self.pending_commands = []
        self.command_num = 0
        self.capabilities = capabilities

    def receiveMsg_Command(self, commandmsg, sender):
        commandmsg.sender = sender
        self.pending_commands.append(commandmsg)
        if len(self.pending_commands) == 1:
            return self._start_command()
        return self._return_watched()

    def receiveMsg_CommandAbort(self, abortmsg, sender):
        if not getattr(self, 'p', None):
            return None
        command = self.pending_commands[-1]
        command.timeout = timedelta(milliseconds=1)
        return self._timed_watch_for_completion(command)

    def _return_watched(self):
        subp = getattr(self, 'p', None)
        if not subp:
            return None
        try:
            return ThespianWatch([subp.stdout.fileno(), subp.stderr.fileno()])
        except IOError:
            return self_finished_command() # command must have finished just now

    def _set_command_timeout(self, command):
        if command.timeout:
            command.expiration = datetime.now() + command.timeout
            self.wakeupAfter(command.timeout, payload=self.command_num)

    def _log(self, command, level, msg, *args):
        if command.logger:
            if isinstance(command.logger, ActorAddress):
                self.send(command.logger, CommandLog(level, msg, *args))
        elif command.logger is None:
            getattr(logging, level)(msg, *args)

    def _start_command(self):
        can_watch = self.capabilities.get('Thespian Watch Supported', False)
        command = self.pending_commands[-1]
        self.command_num += 1
        logcmd = command.exe + ' ' + ' '.join(command.args)
        if command.omit_string:
            logcmd = logcmd.replace(command.omit_string, "...")
        self._log(command, "info", command.logtag + " CMD: " + logcmd)
        self.input_open = command.input_src
        self.start_time = datetime.now()
        self.output = { 'normal': '', 'normal_fh': '', 'error': '', 'error_fh': '' }
        try:
            self.p = subprocess.Popen([command.exe] + command.args,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE,
                                      stdin=subprocess.PIPE,
                                      bufsize=0 if can_watch else (command.max_bufsize or 0),
                                      env=command.env,
                                      shell=command.use_shell)
        except OSError as ex:
            # Error running the executable
            self._add_output(command, 'error', str(ex) + '\n')
            return self._finished_command(ex.errno)
        if command.report_on_start:
            self.send(command.sender, CommandStarted(command, self.p.pid))
        if command.input_src:
            try:
                try:
                    self.p.stdin.write(command.input_src)
                except TypeError:
                    self.p.stdin.write(command.input_src.encode('utf-8'))
            except BrokenPipeError:
                pass
            except OSError as ex:
                if ex.errno == errno.EINVAL and self.p.poll() is not None:
                    pass  # Windows: fails w/EINVAL if proc already exited
                else:
                    raise
            try:
                self.p.stdin.flush()
            except BrokenPipeError:
                pass
            except OSError as ex:
                if ex.errno == errno.EINVAL and self.p.poll() is not None:
                    pass  # Windows: fails w/EINVAL if proc already exited
                else:
                    raise
            if can_watch:
                self.p.stdin.close()  # <-- magic.1: do this or the output gets hung
                self.input_open = False
        else:
            if can_watch:
                self.p.stdin.close()
        if can_watch:
            # magic.2: must nonblock these to allow reading from
            # subprocess before it is shutdown.
            fcntl.fcntl(self.p.stdout.fileno(), fcntl.F_SETFL,
                        fcntl.fcntl(self.p.stdout.fileno(), fcntl.F_GETFL) | os.O_NONBLOCK)
            fcntl.fcntl(self.p.stderr.fileno(), fcntl.F_SETFL,
                        fcntl.fcntl(self.p.stderr.fileno(), fcntl.F_GETFL) | os.O_NONBLOCK)
        return self._timed_watch_for_completion(command)

    def _timed_watch_for_completion(self, command):
        can_watch = self.capabilities.get('Thespian Watch Supported', False)
        if can_watch:
            self._set_command_timeout(command)
            return self._return_watched()
        # This Thespian base does not support ThespianWatch, so this
        # will have to use a blocking wait on the command completion
        if command.timeout:
            end_time = datetime.now() + command.timeout
            while datetime.now() < end_time:
                time.sleep(0.5)
                if self.p.poll():
                    break
            if datetime.now() >= end_time:
                self.p.terminate()
                end_time += timedelta(seconds=2)
                while datetime.now() < end_time:
                    if self.p.poll():
                        break
                    time.sleep(0.5)
                if not self.p.poll():
                    self.p.kill()
                    time.sleep(0.5)
        out, err = self.p.communicate(None)
        self._add_output(self.pending_commands[-1], 'normal', out)
        self._add_output(self.pending_commands[-1], 'error', err)
        self._finished_command()

    def receiveMsg_WatchMessage(self, watchmsg, sender):
        subp = getattr(self, 'p', None)
        if subp:  # and self.pending_commands?
            # n.b. output read from the pipes is a byte string, in an
            # unknown encoding, although with older python, they could
            # also be strings
            if not self.pending_commands:
                return
            for each in watchmsg.ready:
                if each == subp.stdout.fileno():
                    self._add_output(self.pending_commands[-1],
                                     'normal', subp.stdout.read(8192))
                elif each == subp.stderr.fileno():
                    self._add_output(self.pending_commands[-1],
                                     'error', subp.stderr.read(8192))
            if subp.poll() is not None:
                return self._finished_command()
        return self._return_watched()

    def _add_output(self, command, outmark, new_output):
        if not new_output:
            return
        self.output[outmark] += str_form(new_output)
        if command.max_bufsize and \
           len(self.output[outmark]) + \
           len(self.output[outmark+'_fh']) > command.max_bufsize:
            if not self.output[outmark+'_fh']:
                self.output[outmark+'_fh'] = self.output[outmark][:command_max_bufsize/2]
                self.output[outmark] = self.output[len(self.output[outmark+'_fh']):]
            self.output = self.output[-command.max_bufsize-len(self.firsth_output):]
        updates_to = self.pending_commands[-1].output_updates
        if isinstance(updates_to, ActorAddress):
            self.send(updates_to, (CommandOutput
                                   if outmark is 'normal' else
                                   CommandError)(command, new_output))
        if outmark is 'normal':
            self._log_normal_output(new_output)
        elif outmark is 'error':
            self._log_error_output(new_output)

    def _log_normal_output(self, new_output):
        # Logs the first HALF_NUM_LINES_LOGGED output lines, followed
        # by an elision mark, followed by the last
        # HALF_NUM_LINES_LOGGED lines of output (overlapping
        # properly), skipping blank lines, tagging appropriately.
        self._noli = self._log__output(
            'normal', getattr(self, '_noli', None), ' OUT| ',
            self.pending_commands[-1], "info")

    def _log_error_output(self, new_output):
        self._eoli = self._log__output(
            'error', getattr(self, '_eoli', None), ' ERR> ',
            self.pending_commands[-1],
            "info" if self.pending_commands[-1].error_ok else "error")

    def _log__output(self, outmark, oli, pfx, command, level):
        if oli and oli['cmdnum'] == self.command_num and \
           oli['nlines'] >= HALF_NUM_LINES_LOGGED:
            return
        if not oli or oli['cmdnum'] != self.command_num:
            oli = {'cmdnum': self.command_num, 'nbytes': 0, 'nlines': 0,}
        # Assumes that the first HALF_NUM_LINES_LOGGED lines is <
        # command.max_bufsize / 2
        for li in range(oli['nbytes'], len(self.output[outmark])):
            if '\n' == self.output[outmark][li]:
                lline = self.output[outmark][oli['nbytes']:li].strip()
                if lline:
                    self._log(command, level,
                              command.logtag + pfx +
                              (lline.replace(command.omit_string, '...')
                               if command.omit_string else lline))
                oli['nbytes'] += li - oli['nbytes'] + 1
                oli['nlines'] += 1
                if oli['nlines'] == HALF_NUM_LINES_LOGGED:
                    break
        return oli

    def _finished_command(self, errorcode=None):
        command = self.pending_commands[-1]
        subp = getattr(self, 'p', None)
        result = CommandResult(command,
                               errorcode or
                               (-4 if not subp or subp.returncode is None else subp.returncode),
                               ((self.output['normal_fh'],
                                 self.output['normal'])
                                if self.output['normal_fh'] else
                                self.output['normal']),
                               ((self.output['error_fh'],
                                 self.output['error'])
                                if self.output['error_fh'] else
                                self.output['error']),
                               datetime.now() - self.start_time)
        self.pending_commands.pop()
        self.output = None
        self.input_open = False
        self.p = None
        self._log_finished_command(result)
        self.send(command.sender, result)
        if self.pending_commands:
            return self._start_command()
        return self._return_watched()

    def _log_finished_command(self, result):
        normal_out = result.stdout
        if isinstance(normal_out, tuple):
            normal_out = normal_out[1]
        else:
            normal_out = normal_out[getattr(self, '_noli', {}).get('nbytes', 0):]
        nelided = False
        for ni in range(0, len(normal_out)):
            if '\n' == normal_out[-ni-1]:
                lno = list(filter(None, normal_out[-ni:].split('\n')))
                if len(lno) == HALF_NUM_LINES_LOGGED:
                    nelided = ni != len(normal_out)
                    break
        else:
            lno = list(filter(None, normal_out.split('\n')))

        error_out = result.stderr
        if isinstance(error_out, tuple):
            error_out = error_out[1]
        else:
            error_out = error_out[(getattr(self, '_eoli', {}) or {}).get('nbytes', 0):]
        eelided = False
        for ei in range(0, len(error_out)):
            if '\n' == error_out[-ei-1]:
                leo = list(filter(None, error_out[-ei:].split('\n')))
                if len(leo) == HALF_NUM_LINES_LOGGED:
                    nelided = ei != len(error_out)
                    break
        else:
            leo = list(filter(None, error_out.split('\n')))

        lognormal = lambda msg, *args: self._log(result.command, 'info',
                                                 msg, *args)
        logerror = lambda msg, *args: self._log(result.command,
                                                'info'
                                                if result.command.error_ok
                                                else 'error',
                                                msg, *args)
        for each in lno:
            lognormal(result.command.logtag + ' OUT| ' +
                      (each.replace(result.command.omit_string, '...')
                       if result.command.omit_string else each))
        for each in leo:
            logerror(result.command.logtag + ' ERR> ' +
                     (each.replace(result.command.omit_string, '...')
                      if result.command.omit_string else each))
        if result.exitcode:
            logerror(result.command.logtag + ' ERROR exit code: %d' % result.exitcode)
        else:
            lognormal(result.command.logtag + ' completed successfully')

    def receiveMsg_WakeupMessage(self, wakemsg, sender):
        if not self.pending_commands:
            return
        if wakemsg.payload != self.command_num:
            # This wakeup was from a different command; ignore it
            return self._return_watched()
        command = self.pending_commands[-1]
        subp = getattr(self, 'p', None)
        if not subp:
            return  # should never happen
        if subp.poll() is not None:
            return self._finished_command()
        if getattr(self, 'tried_terminate', False):
            subp.kill()
            time.sleep(0.5)  # Final wait for kill
            if subp.poll() is None:
                self._log(command, 'error',
                          tag + " Unable to stop PID %s", subp.pid)
            return self._finished_command()
        self.tried_terminate = True
        subp.terminate()
        self.wakeupAfter(timedelta(seconds=2))
        return self._return_watched()

    def receiveMsg_ActorExitRequest(self, exitmsg, sender):
        subp = getattr(self, 'p', None)
        if subp:
            subp.terminate()

        for each in self.pending_commands[:-1]:
            self.send(each.sender, CommandResult(each, -3, '', '', None))

        if subp and subp.poll() is None:
            time.sleep(0.1)
            subp.kill()
            time.sleep(0.5)
            if subp.poll() is None:
                self._log(command, 'error',
                          tag + " Unable to cancel PID %s", subp.pid)

        if self.pending_commands:
            self._finished_command()
