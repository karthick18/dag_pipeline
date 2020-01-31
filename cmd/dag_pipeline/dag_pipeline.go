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

func testPipelineOp(pipelineInstance *pipeline.Pipeline, pipelineName string) {
	pipelineOps, err := pipelineInstance.PipelineOperations(pipelineName)
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
			// fmt.Println(name, "WAIT ON", w)
			// wait on them to close
			<-channelMap[w]
		}
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
	if _, ok := channelMap[pipelineName]; !ok {
		// Should be a field. Get the pipelines mapped and wait on them.
		pipelines, err := pipelineInstance.GetPipelines([]string{pipelineName})
		if err != nil {
			panic(err.Error())
		}
		for _, p := range pipelines {
			if _, ok := channelMap[p]; ok {
				<-channelMap[p]
			}
		}
	} else {
		<-channelMap[pipelineName]
	}
}

func testPipeline(pipelineConfig []pipeline.PipelineConfig, pipelineName string) {
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
	testPipelineOp(pipelineInstance, pipelineName)
}

func main() {
	pipeline_config := []pipeline.PipelineConfig{
		pipeline.PipelineConfig{Name: "FILE_IMPORTER", Outputs: []string{"mac", "port", "vlan", "vsid", "type"}, Inputs: []string{}},
		pipeline.PipelineConfig{Name: "DHCP", Outputs: []string{"dhcp_ip", "dhcp_hostname"}, Inputs: []string{"mac"}},
		pipeline.PipelineConfig{Name: "DNS", Inputs: []string{"mac", "dhcp_ip"}, Outputs: []string{"dns_hostname"}},
		pipeline.PipelineConfig{Name: "VENDOR", Outputs: []string{"vendor"}, Inputs: []string{"mac"}},
		pipeline.PipelineConfig{Name: "MAC_CAPTURE", Outputs: []string{"mactable"}, Inputs: []string{"mac"}},
		pipeline.PipelineConfig{Name: "NESSUS", Outputs: []string{"NONE_CVE", "CRITICAL_CVE", "MEDIUM_CVE", "INFO_CVE"},
			Inputs: []string{"dhcp_ip", "dhcp_hostname", "dns_hostname"}},
		pipeline.PipelineConfig{Name: "NESSUS_HOST_NETWORK_SCAN", Outputs: []string{"CRITICAL_NETWORK_RISK"},
			Inputs: []string{"dhcp_ip", "CRITICAL_CVE"}},
		pipeline.PipelineConfig{Name: "LEARNING_ML", Outputs: []string{"riskscore"},
			Inputs: []string{"mac", "dhcp_ip", "dhcp_hostname", "dns_hostname", "vendor", "vlan", "port", "mactable", "CRITICAL_NETWORK_RISK"}},
		pipeline.PipelineConfig{Name: "STOP", Outputs: []string{"db"}, Inputs: []string{"LEARNING_ML"}},
		// connect another node handling the same output with a higher weight
		pipeline.PipelineConfig{Name: "VENDOR2", Outputs: []string{"vendor"}, Inputs: []string{"mac"}, Weight: 10},
	}
	pipelines := []string{}
	for _, p := range pipeline_config {
		pipelines = append(pipelines, p.Name)
	}
	pipelineName := "STOP"
	help_str := fmt.Sprintf("Specify pipeline name to test pipeline operation. Available pipelines %v", pipelines)
	flag.StringVar(&pipelineName, "pipeline", pipelineName, help_str)
	flag.Parse()
	testPipeline(pipeline_config, pipelineName)
}
