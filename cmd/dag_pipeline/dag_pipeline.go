package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/karthick18/dag_pipeline/internal/pkg/pipeline"
)

type DagConfig struct {
	outputs []string
	inputs  []string
	//configure non-zero weight to set update precedence if there are multiple pipelines for same output
	weight int
}

func testPipelineOp(dag_config map[string]DagConfig, pipelineInstance *pipeline.Pipeline, pipelineName string) {
	ops, err := pipelineInstance.PipelineOperations(pipelineName)
	if err != nil {
		panic(err.Error())
	}
	channelMap := map[string]chan bool{}
	rand.Seed(time.Now().UnixNano())
	// simulate the runtime of the pipeline
	runCommand := func(name string) {
		defer func() {
			if ch, ok := channelMap[name]; ok {
				close(ch)
			}
		}()
		time.Sleep(time.Duration(rand.Intn(5)+1) * time.Second)
		fmt.Println("RUN", name)
	}
	//simulate waiting on completion of dependent pipelines
	waitCommand := func(name string, waitPipelines []string) {
		// if we have channels, we wait on them to close
		for _, w := range waitPipelines {
			if _, ok := channelMap[w]; !ok {
				continue
			}
			//fmt.Println(name, "WAIT ON", w)
			// wait on them to close
			<-channelMap[w]
		}
	}
	//sanitize the pipelines to created pipelines (ignoring the fields)
	pipelineOps := []pipeline.PipelineOp{}
	for _, pipelineOp := range ops {
		if _, ok := dag_config[pipelineOp.Name]; !ok {
			continue
		}
		waitPipelines := []string{}
		for _, waitPipeline := range pipelineOp.WaitPipelines {
			if _, ok := dag_config[waitPipeline]; ok {
				waitPipelines = append(waitPipelines, waitPipeline)
			}
		}
		pipelineOp.WaitPipelines = waitPipelines
		pipelineOps = append(pipelineOps, pipelineOp)
	}
	// create channels for all pipeline ops
	for _, pipelineOp := range pipelineOps {
		if _, ok := channelMap[pipelineOp.Name]; !ok {
			fmt.Println("Creating", pipelineOp.Name)
			channelMap[pipelineOp.Name] = make(chan bool, 1)
		}
	}
	for _, pipelineOp := range pipelineOps {
		if len(pipelineOp.WaitPipelines) == 0 {
			go runCommand(pipelineOp.Name)
		} else {
			go func(pipelineOp pipeline.PipelineOp) {
				waitCommand(pipelineOp.Name, pipelineOp.WaitPipelines)
				runCommand(pipelineOp.Name)
			}(pipelineOp)
		}
	}
	// wait for pipeline completion
	<-channelMap[pipelineName]
}

func testPipeline(dag_config map[string]DagConfig, pipelineName string) {
	var pipelineConfig []pipeline.PipelineConfig
	for p, config := range dag_config {
		pipelineConfig = append(pipelineConfig, pipeline.PipelineConfig{Name: p, Inputs: config.inputs, Outputs: config.outputs, Weight: config.weight})
	}
	pipelineInstance, err := pipeline.New(pipelineConfig)
	if err != nil {
		panic(err.Error())
	}
	pipelines := pipelineInstance.Get(pipelineName)
	fmt.Println("Get pipeline", pipelineName, pipelines)
	for index, pipeline := range pipelines {
		fmt.Printf("Pipeline %v at level %d\n", pipeline, index)
	}
	fmt.Println("--------")
	fmt.Println("Testing pipeline operations to stop pipeline", pipelineName)
	testPipelineOp(dag_config, pipelineInstance, pipelineName)
}

func main() {
	dag_config := map[string]DagConfig{}
	dag_config["FILE_IMPORTER"] = DagConfig{outputs: []string{"mac", "port", "vlan", "vsid", "type"}, inputs: []string{}}
	dag_config["DHCP"] = DagConfig{outputs: []string{"dhcp_ip", "dhcp_hostname"},
		inputs: []string{"mac"}}
	dag_config["DNS"] = DagConfig{inputs: []string{"mac", "dhcp_ip"},
		outputs: []string{"dns_hostname"}}
	dag_config["VENDOR"] = DagConfig{outputs: []string{"vendor"},
		inputs: []string{"mac"}}
	dag_config["MAC_CAPTURE"] = DagConfig{outputs: []string{"mactable"},
		inputs: []string{"mac"}}
	dag_config["NESSUS"] = DagConfig{outputs: []string{"NONE_CVE", "CRITICAL_CVE", "MEDIUM_CVE", "INFO_CVE"},
		inputs: []string{"dhcp_ip", "dhcp_hostname", "dns_hostname"}}
	dag_config["NESSUS_HOST_NETWORK_SCAN"] = DagConfig{outputs: []string{"CRITICAL_NETWORK_RISK"},
		inputs: []string{"dhcp_ip", "CRITICAL_CVE"}}
	dag_config["LEARNING_ML"] = DagConfig{outputs: []string{"riskscore"},
		inputs: []string{"mac", "dhcp_ip", "dhcp_hostname", "dns_hostname", "vendor", "vlan", "port", "mactable", "CRITICAL_NETWORK_RISK"}}
	dag_config["STOP"] = DagConfig{outputs: []string{"db"},
		inputs: []string{"LEARNING_ML"}}
	// connect another node handling the same output with a higher weight
	dag_config["VENDOR2"] = DagConfig{outputs: []string{"vendor"},
		inputs: []string{"mac"}, weight: 10}
	pipelines := []string{}
	for p := range dag_config {
		pipelines = append(pipelines, p)
	}
	pipelineName := "STOP"
	help_str := fmt.Sprintf("Specify pipeline name to test pipeline operation. Available pipelines %v", pipelines)
	flag.StringVar(&pipelineName, "pipeline", pipelineName, help_str)
	flag.Parse()
	testPipeline(dag_config, pipelineName)
}
