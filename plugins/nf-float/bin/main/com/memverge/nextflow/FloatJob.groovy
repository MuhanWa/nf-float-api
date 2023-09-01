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

import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import com.google.gson.Gson;
import com.google.gson.JsonObject
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

import java.util.regex.Matcher

enum FloatStatus {
    PENDING,
    RUNNING,
    DONE,
    ERROR,
    UNKNOWN,


    static private Map<String, FloatStatus> STATUS_MAP = [
            'Submitted'        : PENDING,
            'Initializing'     : PENDING,
            'Starting'         : RUNNING,
            'Executing'        : RUNNING,
            'Floating'         : RUNNING,
            'Suspended'        : RUNNING,
            'Suspending'       : RUNNING,
            'Resuming'         : RUNNING,
            'Capturing'        : RUNNING,
            'Completed'        : DONE,
            'Cancelled'        : ERROR,
            'Cancelling'       : ERROR,
            'FailToComplete'   : ERROR,
            'FailToExecute'    : ERROR,
            'CheckpointFailed' : ERROR,
            'WaitingForLicense': ERROR,
            'Timedout'         : ERROR,
            'NoAvailableHost'  : ERROR,
            'Unknown'          : UNKNOWN,
    ]

    static FloatStatus of(String status) {
        return STATUS_MAP.getOrDefault(status, UNKNOWN)
    }

    boolean isRunning() {
        return this == PENDING || this == RUNNING
    }

    boolean isFinished() {
        return this == ERROR || this == DONE
    }

    boolean isError() {
        return this == ERROR
    }
}

@Slf4j
class FloatJob {
    String nfJobID
    String floatJobID
    FloatStatus status
    String rc

    private FloatJob() {}

    public FloatJob(JobRest i){
        status = FloatStatus.of(i.status)
        def tags = i.customTags as Map
        nfJobID = tags ? tags[FloatConf.NF_JOB_ID] : ""
        floatJobID = i.id as String
        rc = i.rc ? i.rc : ""

    }

    Integer getRcCode() {
        try {
            return Integer.parseInt(rc)
        } catch (NumberFormatException e) {
            log.debug "parse rc failed ${e.message}"
            return null
        }
    }

    static FloatJob parse(JobRest input) {
        return new FloatJob(input)
    }

    static String getMatch(Matcher matcher) {
        if (matcher.size() == 1) {
            def match = matcher[0]
            if (match.size() > 1) {
                return match[1]
            }
        }
        return ""
    }

    boolean isRunning() {
        return status ? status.isRunning() : false
    }

    boolean isFinished() {
        return status ? status.isFinished() : false
    }

    static Map<String, FloatJob> parseJobMap(String input) {
        Map<String, FloatJob> ret = new HashMap<>()
        try {
            Gson gson = new Gson()
            JsonObject inputMap = gson.fromJson(input,JsonObject.class)
            String jobs = inputMap.get("Jobs")
            Type listType = new TypeToken<List<JobRest>>(){}.getType()
            List<JobRest> jobArr = gson.fromJson(jobs,listType)

            for (i in jobArr) {
                def status = i.status as String
                def tags = i.customTags as Map
                String nfJobID = tags ? tags[FloatConf.NF_JOB_ID] : ""
                def floatJobID = i.id as String
                if (nfJobID && status && nfJobID) {
                    def job = new FloatJob()
                    job.nfJobID = nfJobID
                    job.floatJobID = floatJobID
                    job.status = FloatStatus.of(status)
                    job.rc = i.rc ? i.rc : ""
                    ret[nfJobID] = job
                }
            }
        } catch (Exception e) {
            log.warn "failed to parse: ${input}, detail: ${e.message}"
        }
        return ret
    }
}