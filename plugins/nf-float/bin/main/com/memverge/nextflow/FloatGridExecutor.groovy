/*
 * Copyright 2022-2023, MemVerge Corporation
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

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.util.logging.Slf4j

import nextflow.exception.AbortOperationException
import nextflow.executor.Executor
import nextflow.file.FileHelper
import nextflow.fusion.FusionHelper
import nextflow.processor.TaskRun
import nextflow.util.Escape
import nextflow.util.ServiceName

import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskId
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskProcessor
import nextflow.util.Duration
import nextflow.util.Throttle
//import org.apache.commons.lang.StringUtils

import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

/**
 * Float Executor with a shared file system
 */
@Slf4j
@ServiceName('float')
@CompileStatic
class FloatGridExecutor extends Executor {

    protected Duration queueInterval

    private final static List<String> INVALID_NAME_CHARS = [ " ", "/", ":", "@", "*", "?", "\\n", "\\t", "\\r" ]

    private Map lastQueueStatus

    private static final int DFT_MEM_GB = 1
    private static final int FUSION_MIN_VOL_SIZE = 80
    private static final int MIN_VOL_SIZE = 40

    private FloatJobs _floatJobs

    private AtomicInteger serial = new AtomicInteger()

    private Path binDir

    private FloatClient client

    private FloatConf getFloatConf() {
        return FloatConf.getConf(session.config)
    }

    FloatClient getClient(){
        return client
    }

    FloatJobs getFloatJobs() {
        if (_floatJobs == null) {
            _floatJobs = new FloatJobs(floatConf.addresses, getClient())
        }
        return _floatJobs
    }

    @Override
    protected void register() {
        super.register()
        queueInterval = session.getQueueStatInterval(name)
        log.debug "Creating executor '$name' > queue-stat-interval: ${queueInterval}"
        uploadBinDir()
        client = new FloatClient(floatConf.username, floatConf.password,"")
    }

    TaskMonitor createTaskMonitor() {
        return TaskPollingMonitor.create(session, name, 100, Duration.of('5 sec'))
    }

    FloatTaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir

        new FloatTaskHandler(task, this)
    }

    protected BashWrapperBuilder createBashWrapperBuilder(TaskRun task) {
        // creates the wrapper script
        final builder = new BashWrapperBuilder(task)
        // job directives headers
        builder.headerScript = getHeaderScript(task)
        return builder
    }

    protected String getHeaderScript(TaskRun task) {
        log.info "[float] switch task ${task.id} to ${task.workDirStr}"
        floatJobs.setWorkDir(task.id, task.workDir)

        final path = Escape.path(task.workDir)
        def result = "NXF_CHDIR=${path}\n"

        if (needBinDir()) {
            // add path to the script
            result += "export PATH=\$PATH:${binDir}/bin\n"
        }

        return result
    }

    String getHeaders( TaskRun task ) {

        final token = getHeaderToken()
        def result = new StringBuilder()
        def header = new ArrayList(2)
        def dir = getDirectives(task)
        def len = dir.size()-1
        for( int i=0; i<len; i+=2) {
            def opt = dir[i]
            def val = dir[i+1]
            if( opt ) header.add(opt)
            if( val ) header.add(wrapHeader(val))

            if( header ) {
                result << token << ' ' << header.join(' ') << '\n'
            }

            header.clear()
        }

        return result.toString()
    }

    protected String getHeaderToken() {
        ""
    }

    final List<String> getDirectives(TaskRun task) {
        return new ArrayList<>()
    }

    protected String getJobNameFor(TaskRun task) {

        // -- check for a custom `jobName` defined in the nextflow config file
        def customName = resolveCustomJobName(task)
        if( customName )
            return sanitizeJobName(customName)

        // -- if not available fallback on the custom naming strategy

        final result = new StringBuilder("nf-")
        final name = task.getName()
        for( int i=0; i<name.size(); i++ ) {
            final ch = name[i]
            result.append( INVALID_NAME_CHARS.contains(ch) ? "_" : ch )
        }
        return sanitizeJobName(result.toString())
    }

    protected String sanitizeJobName(String name) {
        name.size() > 256 ? name.substring(0,256) : name
    }

    @PackageScope
    String resolveCustomJobName(TaskRun task) {
        try {
            def custom = (Closure)session?.getExecConfigProp(name, 'jobName', null)
            if( !custom )
                return null

            def ctx = [ (TaskProcessor.TASK_CONTEXT_PROPERTY_NAME): task.config ]
            custom.cloneWith(ctx).call()?.toString()
        }
        catch( Exception e ) {
            log.debug "Unable to resolve job custom name", e
            return null
        }
    }

    private boolean needBinDir() {
        return session.binDir &&
                !session.binDir.empty() &&
                !session.disableRemoteBinDir
    }

    private void uploadBinDir() {
        if (needBinDir()) {
            binDir = getTempDir()
            log.info "Uploading local `bin` ${session.binDir} " +
                    "to ${binDir}/bin"
            FileHelper.copyPath(
                    session.binDir,
                    binDir.resolve("bin"),
                    StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private static String getMemory(TaskRun task) {
        final mem = task.config.getMemory()
        final giga = mem?.toGiga()
        if (!giga) {
            log.debug "memory $mem is too small.  " +
                    "will use default $DFT_MEM_GB"
        }
        return giga ? giga.toString() : DFT_MEM_GB
    }

    private Collection<String> getExtra(TaskRun task) {
        final extraNode = task.config.extra
        def extra = extraNode ? extraNode as String : ''
        final common = floatConf.commonExtra
        if (common) {
            extra = common.trim() + " " + extra.trim()
        }
        def ret = extra.split('\\s+')
        return ret.findAll { it.length() > 0 }
    }

    public String getAddress(){
        final i = serial.incrementAndGet()
        final addresses = floatConf.addresses
        final address = addresses[i % (addresses.size())]
        return address + ":443"
    }

    FloatJob getJob(TaskId taskId) {
        def nfJobID = floatJobs.getNfJobID(taskId)
        def job = floatJobs.nfJobID2job.get(nfJobID)
        if (job == null) {
            return null
        }

        try {
            final addr = getAddress()
            client.setUrl(addr)
            final JobRest res = client.getJobStatus(job.floatJobID)

            if (res != null) {
                job = FloatJob.parse(res)
            }else{
                throw new Exception("Fail to get job status")
            }
            log.debug("Job status : ${job.status}".toString())
        } catch (Exception e) {
            log.warn "[float] failed to retrieve job status $nfJobID, float: ${job.floatJobID}", e
        }
        return job
    }

    String toLogStr(List<String> floatCmd) {
        def ret = floatCmd.join(" ")
        final toReplace = [
                ("-p " + floatConf.password): "-p ***",
                (floatConf.s3accessKey)     : "***",
                (floatConf.s3secretKey)     : "***",
        ]
        for (def entry : toReplace.entrySet()) {
            if (!entry.key) {
                continue
            }
            ret = ret.replace(entry.key, entry.value)
        }
        return ret
    }

    private static def warnDeprecated(String deprecated, String replacement) {
        log.warn1 "[float] process `$deprecated` " +
                "is no longer supported, " +
                "use $replacement instead"
    }

    private static def validate(TaskRun task) {
        if (task.config.nfs) {
            warnDeprecated('nfs', '`float.nfs` config option')
        }
        if (task.config.cpu) {
            warnDeprecated('cpu', '`cpus` directive')
        }
        if (task.config.mem) {
            warnDeprecated('mem', '`memory` directive')
        }
        if (task.config.image) {
            warnDeprecated('image', '`container` directive')
        }
    }

    private List<String> getMountVols(TaskRun task) {
        if (isFusionEnabled()) {
            return []
        }

        List<String> volumes = []
        volumes << floatConf.getWorkDirVol(workDir.uri)

        for (def src : task.getInputFilesMap().values()) {
            volumes << floatConf.getInputVolume(src.uri)
        }
        def ret = volumes.unique() - ""
        log.info "[float] volumes to mount for ${task.id}: ${toLogStr(ret)}"
        return ret
    }

    private Map<String, String> getEnv(FloatTaskHandler handler) {
        return isFusionEnabled()
                ? handler.fusionLauncher().fusionEnv()
                : [:]
    }

    private Map<String, String> getCustomTags(TaskRun task) {
        final result = new LinkedHashMap<String, String>(10)
        result[FloatConf.NF_JOB_ID] = floatJobs.getNfJobID(task.id)
        result[FloatConf.NF_SESSION_ID] = "uuid-${session.uniqueId}".toString()
        result[FloatConf.NF_TASK_NAME] = task.name
        if (task.processor.name) {
            result[FloatConf.NF_PROCESS_NAME] = task.processor.name
        }
        if (session.runName) {
            result[FloatConf.NF_RUN_NAME] = session.runName
        }
        final resourceLabels = task.config.getResourceLabels()
        if (resourceLabels)
            result.putAll(resourceLabels)
        return result
    }

    String getJobSpec(FloatTaskHandler handler, Path scriptFile) {
        final task = handler.task

        validate(task)
        JobSpec spec = new JobSpec()

        final container = task.getContainer()
        if (!container) {
            throw new AbortOperationException("container is empty." +
                    "you can specify a default container image " +
                    "with `process.container`")
        }
        for (def vol : getMountVols(task)) {
            spec.addToDataVolumes(vol)
        }
        spec.setEnvVars(getEnv(handler))
        spec.setImage(task.getContainer())
        spec.setCpu(Integer.valueOf(task.config.getCpus().toString()))
        spec.setMem(Integer.valueOf(getMemory(task)))
        if (task.config.getMachineType()) {
            spec.setInstType(task.config.getMachineType())
        }
        spec.setCustomTags(getCustomTags(task))
        spec.setImageVolSize(getVolSize(task))
        String path = getScriptFilePath(handler, scriptFile)
        spec.setJobContent(Files.readString(Path.of(path)))
        String specString = spec.toJsonString()
        log.info "[float] creating job: ${specString}"
        return specString
    }

    private String getScriptFilePath(FloatTaskHandler handler, Path scriptFile) {
        if (isFusionEnabled()) {
            return saveFusionScriptFile(handler, scriptFile)
        }
        if (workDir.getScheme() == "s3") {
            return downloadScriptFile(scriptFile)
        }
        return scriptFile.toString()
    }

    protected static String saveFusionScriptFile(FloatTaskHandler handler, Path scriptFile) {
        final localTmp = File.createTempFile("nextflow", scriptFile.name)
        log.info("save fusion launcher script")
        localTmp.text = '#!/bin/bash\n' + handler.fusionSubmitCli().join(' ') + '\n'
        return localTmp.getAbsolutePath()
    }

    protected String downloadScriptFile(Path scriptFile) {
        final localTmp = File.createTempFile("nextflow", scriptFile.name)
        log.info("download $scriptFile to $localTmp")
        FileHelper.copyPath(
                scriptFile,
                localTmp.toPath(),
                StandardCopyOption.REPLACE_EXISTING)
        return localTmp.getAbsolutePath()
    }

    private int getVolSize(TaskRun task) {
        int size = MIN_VOL_SIZE

        def disk = task.config.getDisk()
        if (disk) {
            size = Math.max(size, disk.toGiga().intValue())
        }
        if (isFusionEnabled()) {
            size = Math.max(size, FUSION_MIN_VOL_SIZE)
        }
        return size;
    }

    void killTask(def jobId) {
        def jobIds
        if (jobId instanceof Collection) {
            jobIds = jobId
        } else {
            jobIds = [jobId]
        }
        jobIds.forEach {
            def id = it.toString()
            def url = floatJobs.getOc(id)
            log.info "[float] cancel job: $id"
            client.setUrl(url)
            client.cancelJob(id)
        }
        return
    }

    static protected enum QueueStatus { PENDING, RUNNING, HOLD, ERROR, DONE, UNKNOWN }
    /**
     * @return The status for all the scheduled and running jobs
     */
    
    protected Map<String, QueueStatus> getQueueStatus0(queue) {
        return queueStatus
    }

    Map<String,QueueStatus> getQueueStatus(queue) {
        final global = session.getExecConfigProp(name, 'queueGlobalStatus',false)
        if( global ) {
            log.debug1("Executor '$name' fetching queue global status")
            queue = null
        }
        Map<String,QueueStatus> status = Throttle.cache("${name}_${queue}", queueInterval) {
            final result = getQueueStatus0(queue)
            log.trace "[${name.toUpperCase()}] queue ${queue?"($queue) ":''}status >\n" + dumpQueueStatus(result)
            return result
        }
        // track the last status for debugging purpose
        lastQueueStatus = status
        return status
    }

    protected List<String> getOcs(){
        return floatConf.addresses.stream().collect(Collectors.toList())
    } 

    protected Map<String, QueueStatus> getQueueStatus() {
        final ocs = getOcs()
        floatJobs.updateStatus(ocs)
        log.debug "[float] collecting job status completes."
        return nfJobID2Status
    }

    protected Map<String, QueueStatus> parseQueueStatus(String s) {
        def stMap = floatJobs.parseQStatus(s)
        return toStatusMap(stMap)
    }

    private Map<String, QueueStatus> getNfJobID2Status() {
        Map<String, FloatJob> stMap = floatJobs.getNfJobID2job()
        return toStatusMap(stMap)
    }

    private static Map<String, QueueStatus> toStatusMap(Map<String, FloatJob> stMap) {
        Map<String, QueueStatus> ret = new HashMap<>()
        stMap.forEach { key, job ->
            QueueStatus status = STATUS_MAP.getOrDefault(job.status, QueueStatus.UNKNOWN)
            ret[key] = status
        }
        return ret
    }

    FloatStatus getJobStatus(TaskRun task) {
        def job = getJob(task.id)
        if (!job) {
            return FloatStatus.UNKNOWN
        }
        log.debug "[float] task id: ${task.id}, nf-job-id: $job.nfJobID, " +
                "float-job-id: $job.floatJobID, float status: $job.status"
        if (job.finished) {
            floatJobs.refreshWorkDir(job.nfJobID)
            task.exitStatus = job.rcCode
        }
        return job.status
    }

    static private Map<FloatStatus, QueueStatus> STATUS_MAP = new HashMap<>()

    static {
        STATUS_MAP.put(FloatStatus.PENDING, QueueStatus.PENDING)
        STATUS_MAP.put(FloatStatus.RUNNING, QueueStatus.RUNNING)
        STATUS_MAP.put(FloatStatus.DONE, QueueStatus.DONE)
        STATUS_MAP.put(FloatStatus.ERROR, QueueStatus.ERROR)
        STATUS_MAP.put(FloatStatus.UNKNOWN, QueueStatus.UNKNOWN)
    }

    @PackageScope
    final String dumpQueueStatus(Map<String,QueueStatus> statusMap) {
        if( statusMap == null )
            return '  (null)'
        if( statusMap.isEmpty() )
            return '  (empty)'

        def result = new StringBuilder()
        statusMap?.each { k, v ->
            result << '  job: ' << k?.toString() << ': ' << v?.toString() << '\n'
        }
        return result.toString()
    }

    @PackageScope
    final String dumpQueueStatus() {
        dumpQueueStatus(lastQueueStatus)
    }

    boolean checkStartedStatus(jobId, queueName) {
        assert jobId

        // -- fetch the queue status
        final queue = getQueueStatus(queueName)
        if( !queue )
            return false

        if( !queue.containsKey(jobId) )
            return false
        if( queue.get(jobId) == QueueStatus.PENDING )
            return false

        return true
    }

    boolean checkActiveStatus( jobId, queue ) {
        assert jobId

        // -- fetch the queue status
        final status = getQueueStatus(queue)

        if( status == null ) { // no data is returned, so return true
            return true
        }

        if( !status.containsKey(jobId) ) {
            log.trace "[${name.toUpperCase()}] queue ${queue?"($queue) ":''}status > map does not contain jobId: `$jobId`"
            return false
        }

        final result = status[jobId.toString()] == QueueStatus.RUNNING || status[jobId.toString()] == QueueStatus.HOLD
        log.trace "[${name.toUpperCase()}] queue ${queue?"($queue) ":''}status > jobId `$jobId` active status: $result"
        return result
    }

    protected String wrapHeader( String str ) { str }

    protected String quote(Path path) {
        def str = Escape.path(path)
        path.toString() != str ? "\"$str\"" : str
    }

    protected boolean pipeLauncherScript(){
        return isFusionEnabled()
    }

    @Override
    boolean isFusionEnabled() {
        return FusionHelper.isFusionEnabled(session)
    }

    @Override
    boolean isContainerNative() {
        return true
    }
}
