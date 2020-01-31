package pipeline

import (
	"fmt"
	"sort"
	"sync"
)

/*
 This is a DAG. We have arrows pointing towards us. So that is our incoming vertex
 to which we would be outgoing.
*/
type Vertex struct {
	name   string
	weight int
	// My current level (depth) is the MAX LEVEL of my outgoing vertex/connections + 1.
	level int
	// Incoming vertices to us.
	incoming []*Vertex
	// Outgoing vertices from us.
	outgoing    []*Vertex
	incomingMap map[string]struct{}
	outgoingMap map[string]struct{}
}

type Pipeline struct {
	verticesMap        map[string]*Vertex
	pipelineMap        map[string][]string
	pipelineOutputsMap map[string][]string
	sync.Mutex
}

type PipelineOp struct {
	Name          string
	WaitPipelines []string
}

type PipelineConfig struct {
	Name    string
	Inputs  []string
	Outputs []string
	Weight  int
}

type path struct {
	name   string
	weight int
	level  int
}

func newVertex(name string, weight int) *Vertex {
	v := &Vertex{name: name, weight: weight, level: 0, incomingMap: make(map[string]struct{}), outgoingMap: make(map[string]struct{}), incoming: make([]*Vertex, 0, 0), outgoing: make([]*Vertex, 0, 0)}
	return v
}

func (v *Vertex) addIncoming(i *Vertex) {
	if _, ok := v.incomingMap[i.name]; !ok {
		v.incoming = append(v.incoming, i)
		v.incomingMap[i.name] = struct{}{}
	}
}

func (v *Vertex) addOutgoing(o *Vertex) {
	if _, ok := v.outgoingMap[o.name]; !ok {
		v.outgoing = append(v.outgoing, o)
		v.outgoingMap[o.name] = struct{}{}
	}
}

func (v *Vertex) getLevel() int {
	return v.level
}

// Update the level if its greater than current level.
func (v *Vertex) setLevel(level int) bool {
	if v.level < level {
		v.level = level
		return true
	}
	return false
}

func New(config []PipelineConfig) (*Pipeline, error) {
	pipeline := &Pipeline{verticesMap: make(map[string]*Vertex), pipelineMap: make(map[string][]string), pipelineOutputsMap: make(map[string][]string)}
	// Configure the outputs.
	for _, c := range config {
		if err := pipeline.SetOutputs(c.Name, c.Outputs); err != nil {
			return nil, err
		}
	}
	// Connect the pipelines.
	for _, c := range config {
		if err := pipeline.Add(c.Name, c.Inputs, c.Outputs, c.Weight); err != nil {
			return nil, err
		}
	}
	return pipeline, nil
}

// Configure the outputs for the pipeline.
func (p *Pipeline) SetOutputs(pipeline string, outputs []string) error {
	for _, o := range outputs {
		if err := p.addPipelineToOutput(pipeline, o); err != nil {
			return err
		}
	}
	// Add ourselves to the map.
	if err := p.addPipelineToOutput(pipeline, pipeline); err != nil {
		return err
	}
	p.pipelineOutputsMap[pipeline] = outputs
	return nil
}

// Fork the output in case its connected to multiple pipelines.
func (p *Pipeline) addPipelineToOutput(newPipeline string, output string) error {
	if _, ok := p.pipelineMap[output]; ok {
		for _, pipeline := range p.pipelineMap[output] {
			if pipeline == newPipeline {
				return nil
			}
		}
	}
	p.pipelineMap[output] = append(p.pipelineMap[output], newPipeline)
	return nil
}

func (p *Pipeline) GetPipelines(inputs []string) ([]string, error) {
	pipelines := []string{}
	visitedMap := map[string]struct{}{}
	for _, i := range inputs {
		if v, ok := p.pipelineMap[i]; ok {
			for _, pipeline := range v {
				if _, ok = visitedMap[pipeline]; !ok {
					pipelines = append(pipelines, pipeline)
					visitedMap[pipeline] = struct{}{}
				}
			}
		} else {
			return nil, fmt.Errorf("no pipeline registered for input %s", i)
		}
	}
	return pipelines, nil
}

func (p *Pipeline) checkIncomingVerticesForLoop(vertex *Vertex, loopDetect func(*Vertex, []string) error,
	visitedMap map[string]struct{}, path []string) error {
	if _, ok := visitedMap[vertex.name]; ok {
		return nil
	}
	visitedMap[vertex.name] = struct{}{}
	path = append(path, vertex.name)
	for _, i := range vertex.incoming {
		if err := p.checkIncomingVerticesForLoop(i, loopDetect, visitedMap, path); err != nil {
			return err
		}
	}
	if err := loopDetect(vertex, path); err != nil {
		return err
	}
	path = path[:len(path)-1]
	return nil
}

// Recursively update the levels of all our incoming connections.
func (p *Pipeline) updateLevel(vertex *Vertex, visitedMap map[string]struct{}) {
	if _, ok := visitedMap[vertex.name]; ok {
		return
	}
	visitedMap[vertex.name] = struct{}{}
	for _, i := range vertex.incoming {
		level := vertex.getLevel() + 1
		if i.setLevel(level) {
			p.updateLevel(i, visitedMap)
		}
	}
}

func (p *Pipeline) connect(from string, to string, weight int) error {
	if _, ok := p.verticesMap[from]; !ok {
		p.verticesMap[from] = newVertex(from, weight)
	}
	fromVertex := p.verticesMap[from]
	if _, ok := p.verticesMap[to]; !ok {
		p.verticesMap[to] = newVertex(to, 0)
	}
	toVertex := p.verticesMap[to]
	// Update the weight of the from vertex if different.
	if fromVertex.weight != weight {
		fromVertex.weight = weight
	}
	// Ensure there are no loops.
	loopDetect := func(v *Vertex, path []string) error {
		if v.name == to {
			return fmt.Errorf("loop detected as vertex %s is already incoming vertex to %s on path %v", v.name, from, path)
		}
		return nil
	}
	visitedMap := map[string]struct{}{}
	path := []string{}
	if err := p.checkIncomingVerticesForLoop(fromVertex, loopDetect, visitedMap, path); err != nil {
		return err
	}
	toVertex.addIncoming(fromVertex)
	fromVertex.addOutgoing(toVertex)

	// Update the level of  from_vertex to be the (max) level of our to_vertex + 1.
	level := toVertex.getLevel() + 1

	if fromVertex.setLevel(level) {
		visitedMap = map[string]struct{}{}
		p.updateLevel(fromVertex, visitedMap)
	}
	//fmt.Println("Connected vertex", fromVertex.name, "->", toVertex.name)
	return nil
}

func (p *Pipeline) connectInputOutput(pipeline string, inputs []string, outputs []string, weight int) error {
	p.Lock()
	defer p.Unlock()
	if err := p.SetOutputs(pipeline, outputs); err != nil {
		return err
	}

	// Connect the outputs to the inputs.
	// This is purely there for GetByField as edges are fetched using pipeline names mapped to the outputs.
	// This allows us to trace the edges using field names (in case).
	for _, o := range outputs {
		// Check if there is a pipeline registered and connected for this output.
		// If yes, connect our pipeline to the input fields.
		if pipelines, ok := p.pipelineMap[o]; ok {
			if len(pipelines) > 1 {
				if vertex, ok := p.verticesMap[o]; ok && len(vertex.outgoing) > 0 {
					o = pipeline
				}
			}
		}
		for _, i := range inputs {
			if err := p.connect(o, i, weight); err != nil {
				return err
			}
		}
	}

	// Connect the pipelines to the inputs.
	inputPipelines, err := p.GetPipelines(inputs)
	if err != nil {
		return err
	}
	for _, i := range inputPipelines {
		if err := p.connect(pipeline, i, weight); err != nil {
			return err
		}
	}
	return nil
}

func (p *Pipeline) Add(pipeline string, inputs []string, outputs []string, weight ...int) error {
	w := 0
	if len(weight) > 0 {
		w = weight[0]
	}
	return p.connectInputOutput(pipeline, inputs, outputs, w)
}

func (p *Pipeline) getOutputsFromVertex(v *Vertex, outgoingMap map[string]path) {
	if _, ok := outgoingMap[v.name]; ok {
		return
	}
	// Map the vertex name to the pipeline name.
	if pipelines, ok := p.pipelineMap[v.name]; ok {
		for _, name := range pipelines {
			// Pipeline has to exist in the vertex map.
			vertex := p.verticesMap[name]
			outgoingMap[name] = path{name: vertex.name, weight: vertex.weight, level: vertex.getLevel()}
		}
	} else {
		outgoingMap[v.name] = path{name: v.name, weight: v.weight, level: v.getLevel()}
	}
	for _, o := range v.outgoing {
		p.getOutputsFromVertex(o, outgoingMap)
	}
}

func (p *Pipeline) getOutputs(pipeline string) map[string]path {
	if fromVertex, ok := p.verticesMap[pipeline]; ok {
		outgoingMap := map[string]path{}
		p.getOutputsFromVertex(fromVertex, outgoingMap)
		return outgoingMap
	}
	return nil
}

// Convert the connections map by vertex name and level into an array of pipelines sorted by level.
func (p *Pipeline) connectionsToPipelinesByLevel(connections map[string]path) [][]string {
	var ss []path
	for _, p := range connections {
		ss = append(ss, p)
	}
	sort.Slice(ss, func(i, j int) bool {
		// If levels are same, sort by weight.
		if ss[i].level == ss[j].level {
			return ss[i].weight < ss[j].weight
		}
		return ss[i].level < ss[j].level
	})
	pipelines := make([][]string, 0, ss[len(ss)-1].level+1)
	lastLevel := -1
	index := -1
	for _, path := range ss {
		if path.level != lastLevel {
			index += 1
			pipelines = append(pipelines, []string{})
			lastLevel = path.level
		}
		pipelines[index] = append(pipelines[index], path.name)
	}
	return pipelines
}

func (p *Pipeline) getNoLock(pipeline string) [][]string {
	var connections = map[string]path{}
	if names, ok := p.pipelineMap[pipeline]; ok {
		for _, name := range names {
			outputConnections := p.getOutputs(name)
			if outputConnections != nil {
				// Merge into connections.
				for k, v := range outputConnections {
					connections[k] = v
				}
			}
		}
	}
	if len(connections) == 0 {
		return nil
	}
	return p.connectionsToPipelinesByLevel(connections)
}

// Get all the connected outputs from this pipeline and sort by level.
func (p *Pipeline) Get(pipeline string) [][]string {
	p.Lock()
	defer p.Unlock()
	return p.getNoLock(pipeline)
}

func (p *Pipeline) GetByField(field string) [][]string {
	p.Lock()
	if pipelines, ok := p.pipelineMap[field]; ok && len(pipelines) > 1 {
		p.Unlock()
		return p.Get(field)
	}
	defer p.Unlock()
	connections := p.getOutputs(field)
	if connections == nil {
		return nil
	}
	return p.connectionsToPipelinesByLevel(connections)
}

func (p *Pipeline) pipelinesByLevelToOperationsNoLock(pipelines [][]string) []PipelineOp {
	pipelineOperations := []PipelineOp{}
	if pipelines == nil {
		return pipelineOperations
	}
	for level, pipeline := range pipelines {
		if level == 0 {
			// Nothing to wait.
			for _, name := range pipeline {
				pipelineOperations = append(pipelineOperations, PipelineOp{Name: name})
			}
		} else {
			// Build waitpipeline to wait on outgoing edges before executing pipeline.
			multiplePipelineForSameOutput := map[string][]string{}
			for index, name := range pipeline {
				// Check if there are multiple pipelines attached to same output.
				// If yes, we ensure that the one with the highest weight/precedence is executed last to update the output.
				if outputs, ok := p.pipelineOutputsMap[name]; ok {
					for _, o := range outputs {
						if outputPipelines, ok := p.pipelineMap[o]; ok && len(outputPipelines) > 1 {
							// Add ourselves to the map of others.
							for _, op := range outputPipelines {
								if op != name {
									multiplePipelineForSameOutput[op] = append(multiplePipelineForSameOutput[op], name)
								}
							}
						}
					}
				}
				connections := p.getOutputs(name)
				if connections == nil {
					continue
				}
				waitPipelines := []string{}
				for out := range connections {
					if out != name {
						waitPipelines = append(waitPipelines, out)
					}
				}
				// Check same output multiple pipeline map for wait pipelines.
				// We wait for lower precedence on same output so that the last update on output is from a higher precedence.
				if index != 0 {
					if waitsForSameOutput, ok := multiplePipelineForSameOutput[name]; ok {
						waitPipelines = append(waitPipelines, waitsForSameOutput...)
					}
				}
				pipelineOperations = append(pipelineOperations, PipelineOp{Name: name, WaitPipelines: waitPipelines})
			}
		}
	}

	return pipelineOperations
}

// Convert the pipelines sorted by levels into operations that can be used to execute the pipeline concurrently.
func (p *Pipeline) PipelinesByLevelToOperations(pipelines [][]string) []PipelineOp {
	p.Lock()
	defer p.Unlock()
	return p.pipelinesByLevelToOperationsNoLock(pipelines)
}

// Get the pipeline operations to be executed for the input pipeline.
func (p *Pipeline) PipelineOperations(pipeline string) ([]PipelineOp, error) {
	p.Lock()
	defer p.Unlock()
	pipelines := p.getNoLock(pipeline)
	if pipelines == nil {
		return []PipelineOp{}, fmt.Errorf("No pipeline operations found for pipeline: %s", pipeline)
	}
	return p.pipelinesByLevelToOperationsNoLock(pipelines), nil

}
