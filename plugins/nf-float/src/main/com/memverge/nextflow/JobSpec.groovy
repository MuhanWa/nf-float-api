package com.memverge.nextflow

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class JobSpec {
    public Integer cpu;
    public Integer mem;
    public String instType;
    public String image;
    public Integer imageVolSize;
    public String jobContent;
    public List<String> dataVolumes = new ArrayList<>();
    public Map<String,String> customTags = new HashMap<>();
    public List<String> envs = new ArrayList<>();

    public JobSpec(){
        this.cpu = 2
        this.mem = 4
        this.image = ""
        this.jobContent = ""
        this.instType = ""
        this.imageVolSize = 0
    }

    public void setCpu(int c){
        this.cpu = c
    }

    public void setMem(int m){
        this.mem = m
    }

    public void setInstType(String type){
        this.instType = (type == null) ? "" : type
    }

    public void setImage(String im){
        this.image = im
    }

    public void setImageVolSize(int v){
        this.imageVolSize = v
    }

    public void setJobContent(String file){
        this.jobContent = file
    }

    public void addToDataVolumes(String vol){
        this.dataVolumes.add(vol)
    }

    public void addToCustomTags(String key, String val){
        this.customTags.put(key,val)
    }

    public void setCustomTags(Map<String,String> map){
        this.customTags = map
    }

    public void setEnvVars(Map<String,String> env){
        env.each{key,val ->
            this.envs.push("${key}=${val}".toString())
        }
    }

    public String toJsonString(){
        Gson gson = new Gson();
        return gson.toJson(this)
    }
}