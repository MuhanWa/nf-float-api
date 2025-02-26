/*
 * Copyright 2013-2023, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.memverge.nextflow

import java.nio.file.FileSystems
import java.nio.file.attribute.BasicFileAttributes
import nextflow.executor.BatchCleanup
import static nextflow.processor.TaskStatus.*
import nextflow.trace.TraceRecord

import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.time.temporal.ChronoUnit
import java.util.function.Predicate
import java.util.regex.Pattern

import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.event.EventListener
import dev.failsafe.event.ExecutionAttemptedEvent
import dev.failsafe.function.CheckedSupplier
import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import nextflow.exception.ProcessException
import nextflow.exception.ProcessFailedException
import nextflow.exception.ProcessNonZeroExitStatusException
import nextflow.file.FileHelper
import nextflow.fusion.FusionAwareTask
import nextflow.fusion.FusionHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskHandler
import nextflow.trace.TraceRecord
import nextflow.util.CmdLineHelper
import nextflow.util.Duration
import nextflow.util.Throttle
import nextflow.executor.BashWrapperBuilder

/**
 * Float task handler
 */
@Slf4j
@CompileStatic
class FloatTaskHandler extends TaskHandler implements FusionAwareTask {
    /** The target executor platform */
    final FloatGridExecutor executor

    /** Location of the file created when the job is started */
    final Path startFile

    /** Location of the file created when the job is terminated */
    final Path exitFile

    /** Location of the file holding the task std output */
    final Path outputFile

    /** Location of the file holding the task std error */
    final Path errorFile

    /** The wrapper file used to execute the user script */
    final Path wrapperFile

    /** The unique job ID as provided by the underlying grid platform */
    private jobId

    private queue

    private long exitStatusReadTimeoutMillis

    private Duration sanityCheckInterval

    static private final Duration READ_TIMEOUT = Duration.of('270sec') // 4.5 minutes

    FloatTaskHandler( TaskRun task, FloatGridExecutor executor ) {
        super(task)

        this.executor = executor
        this.startFile = task.workDir.resolve(TaskRun.CMD_START)
        this.exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
        this.outputFile = task.workDir.resolve(TaskRun.CMD_OUTFILE)
        this.errorFile = task.workDir.resolve(TaskRun.CMD_ERRFILE)
        this.wrapperFile = task.workDir.resolve(TaskRun.CMD_RUN)
        final duration = executor.session?.getExitReadTimeout(executor.name, READ_TIMEOUT) ?: READ_TIMEOUT
        this.exitStatusReadTimeoutMillis = duration.toMillis()
        this.queue = task.config?.queue
        this.sanityCheckInterval = duration
    }

    protected String fusionStdinWrapper() {
        return '#!/bin/bash\n' + fusionSubmitCli().join(' ') + '\n'
    }

    protected BashWrapperBuilder createTaskWrapper(TaskRun task) {
        return fusionEnabled()
            ? fusionLauncher()
            : executor.createBashWrapperBuilder(task)
    }

    protected String stdinLauncherScript() {
        return fusionEnabled() ? fusionStdinWrapper() : wrapperFile.text
    }

    protected String submitDirective(TaskRun task) {
        final remoteLog = task.workDir.resolve(TaskRun.CMD_LOG).toString()
        // replaces the log file with a null file because the cluster submit tool
        // cannot write to a file hosted in a remote object storage
        final result = executor
                .getHeaders(task)
                .replaceAll(remoteLog, '/dev/null')
        return result
    }

    protected String launchCmd0(ProcessBuilder builder, String pipeScript) {
        def result = CmdLineHelper.toLine(builder.command())
        if( pipeScript ) {
            result = "cat << 'LAUNCH_COMMAND_EOF' | ${result}\n"
            result += pipeScript.trim() + '\n'
            result += 'LAUNCH_COMMAND_EOF\n'
        }
        return result
    }
    
    @Override
    void submit(){
        createTaskWrapper(task).build()
        final String jobspec = executor.getJobSpec(this, wrapperFile)
        final String url = executor.getAddress()
        log.debug("url: " + url)
        FloatClient client = executor.getClient()
        client.setUrl(url)
        String result = client.submitJob(jobspec)
        if(!result.equals("")){
            jobId = result
            status = SUBMITTED
            log.debug "[${executor.name.toUpperCase()}] submitted process ${task.name} > jobId: $jobId; workDir: ${task.workDir}"
        }else{
            status = COMPLETED
            throw new Exception("Error submitting process '${task.name}' for execution")
        }

    }

    private long startedMillis

    private long exitTimestampMillis0 = System.currentTimeMillis()

    private long exitTimestampMillis1

    private long exitTimestampMillis2

    protected Integer readExitStatus() {

        String workDirList = null
        if( exitTimestampMillis1 && FileHelper.workDirIsNFS ) {
            /*
             * When the file is in a NFS folder in order to avoid false negative
             * list the content of the parent path to force refresh of NFS metadata
             * http://stackoverflow.com/questions/3833127/alternative-to-file-exists-in-java
             * http://superuser.com/questions/422061/how-to-determine-whether-a-directory-is-on-an-nfs-mounted-drive
             */
            workDirList = FileHelper.listDirectory(task.workDir)
        }

        /*
         * when the file does not exist return null, to force the monitor to continue to wait
         */
        BasicFileAttributes exitAttrs = null
        if( !exitFile || !(exitAttrs=FileHelper.readAttributes(exitFile)) || !exitAttrs.lastModifiedTime()?.toMillis() ) {
            if( log.isTraceEnabled() ) {
                if( !exitFile )
                    log.trace "JobId `$jobId` exit file is null"
                else
                    log.trace "JobId `$jobId` exit file: ${exitFile.toUriString()} - lastModified: ${exitAttrs?.lastModifiedTime()} - size: ${exitAttrs?.size()}"
            }
            // -- fetch the job status before return a result
            final active = executor.checkActiveStatus(jobId, queue)

            // --
            def elapsed = System.currentTimeMillis() - startedMillis
            if( elapsed < executor.queueInterval.toMillis() * 2.5 ) {
                return null
            }

            // -- if the job is active, this means that it is still running and thus the exit file cannot exist
            //    returns null to continue to wait
            if( active ) {
                // make sure to reset exit time if the task is active -- see #927
                exitTimestampMillis1 = 0
                return null
            }

            // -- if the job is not active, something is going wrong
            //  * before returning an error code make (due to NFS latency) the file status could be in a incoherent state
            if( !exitTimestampMillis1 ) {
                log.trace "Exit file does not exist for and the job is not running for task: $this -- Try to wait before kill it"
                exitTimestampMillis1 = System.currentTimeMillis()
            }

            def delta = System.currentTimeMillis() - exitTimestampMillis1
            if( delta < exitStatusReadTimeoutMillis ) {
                return null
            }

            def errMessage = []
            errMessage << "Failed to get exit status for process ${this} -- exitStatusReadTimeoutMillis: $exitStatusReadTimeoutMillis; delta: $delta"
            // -- dump current queue stats
            errMessage << "Current queue status:"
            errMessage << executor.dumpQueueStatus()?.indent('> ')
            // -- dump directory listing
            errMessage << "Content of workDir: ${task.workDir}"
            errMessage << workDirList?.indent('> ')
            log.debug errMessage.join('\n')

            return Integer.MAX_VALUE
        }

        /*
         * read the exit file, it should contain the executed process exit status
         */
        def status = exitFile.text?.trim()
        if( status ) {
            try {
                return status.toInteger()
            }
            catch( Exception e ) {
                log.warn "Unable to parse process exit file: ${exitFile.toUriString()} -- bad value: '$status'"
            }
        }

        else {
            /*
             * Since working with NFS it may happen that the file exists BUT it is empty due to network latencies,
             * before returning an invalid exit code, wait some seconds.
             *
             * More in detail:
             * 1) the very first time that arrive here initialize the 'exitTimestampMillis' to the current timestamp
             * 2) when the file is empty but less than 5 seconds are spent from the first check, return null
             *    this will force the monitor to continue to wait for job termination
             * 3) if more than 5 seconds are spent, and the file is empty return MAX_INT as an invalid exit status
             *
             */
            if( !exitTimestampMillis2 ) {
                log.debug "File is returning empty content: $this -- Try to wait a while... and pray."
                exitTimestampMillis2 = System.currentTimeMillis()
            }

            def delta = System.currentTimeMillis() - exitTimestampMillis2
            if( delta < exitStatusReadTimeoutMillis ) {
                return null
            }
            log.warn "Unable to read command status from: ${exitFile.toUriString()} after $delta ms"
        }

        return Integer.MAX_VALUE
    }

    @Override
    boolean checkIfRunning() {

        if( isSubmitted() ) {
            if( isStarted() ) {
                status = RUNNING
                // use local timestamp because files are created on remote nodes which
                // may not have a synchronized clock
                startedMillis = System.currentTimeMillis()
                return true
            }
        }

        return false
    }

    private boolean isStarted() {

        BasicFileAttributes attr
        if( startFile && (attr=FileHelper.readAttributes(startFile)) && attr.lastModifiedTime()?.toMillis() > 0  )
            return true

        // check if the jobId is tracked in the queue status
        if( executor.checkStartedStatus(jobId, queue) )
            return true

        // to avoid unnecessary pressure on the file system check the existence of
        // the exit file on only on a time-periodic basis
        def now = System.currentTimeMillis()
        if( now - exitTimestampMillis0 > exitStatusReadTimeoutMillis ) {
            exitTimestampMillis0 = now
            // fix issue #268
            if( exitFile && (attr=FileHelper.readAttributes(exitFile)) && attr.lastModifiedTime()?.toMillis() > 0  )
                return true
        }

        return false
    }

    @Override
    boolean checkIfCompleted() {
        final FloatStatus st =  executor.getJobStatus(task)
        if (st.finished) {
            status = COMPLETED
            if (task.exitStatus == null) {
                task.exitStatus = readExitStatus()
            }
            // both exit status and job rc code are empty
            if (task.exitStatus == null) {
                if (st.isError()) {
                    task.exitStatus = 1
                } else {
                    task.exitStatus = 0
                }
            }
            task.stdout = outputFile
            task.stderr = errorFile
        }
        return completed
    }

    protected boolean passSanityCheck() {
        Throttle.after(sanityCheckInterval, true) {
            if( isCompleted() ) {
                return true
            }
            if( task.workDir.exists() ) {
                return true
            }
            // if the task is not complete (ie submitted or running)
            // AND the work-dir does not exists ==> something is wrong
            task.error = new ProcessException("Task work directory is missing (!)")
            // sanity check does not pass
            return false
        }
    }

    @Override
    void kill() {
        executor.killTask(jobId)
    }

    protected StringBuilder toStringBuilder( StringBuilder builder ) {
        builder << "jobId: $jobId; "

        super.toStringBuilder(builder)
        final exitAttrs = FileHelper.readAttributes(exitFile)

        builder << " started: " << (startedMillis ? startedMillis : '-') << ';'
        builder << " exited: " << (exitAttrs ? exitAttrs.lastModifiedTime() : '-') << '; '

        return builder
    }

    @Override
    TraceRecord getTraceRecord() {
        def trace = super.getTraceRecord()
        trace.put('native_id', jobId)
        return trace
    }
}
