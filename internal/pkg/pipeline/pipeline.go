package pipeline

import (
	"fmt"
	"sort"
	"sync"
)

// this is a DAG. We have arrows pointing towards us. So that is our incoming vertex
// to which we would be outgoing

type Vertex struct {
	name   string
	weight int
	// my current level (depth) is the MAX LEVEL of my outgoing vertex/connections + 1
	level int
	// incoming vertices to this
	incoming []*Vertex
	// outgoing vertices from this
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

func (v *Vertex) setLevel(level int) bool {
	// update the level if its greater than current level
	if v.level < level {
		v.level = level
		return true
	}
	return false
}

func New() *Pipeline {
	pipeline := &Pipeline{verticesMap: make(map[string]*Vertex), pipelineMap: make(map[string][]string), pipelineOutputsMap: make(map[string][]string)}
	return pipeline
}

func (p *Pipeline) SetOutputs(pipeline string, outputs []string) error {
	for _, o := range outputs {
		if err := p.addOutputToPipeline(pipeline, o); err != nil {
			return err
		}
	}
	// add self to the map
	if err := p.addOutputToPipeline(pipeline, pipeline); err != nil {
		return err
	}
	p.pipelineOutputsMap[pipeline] = outputs
	return nil
}

// fork the output in case its connected to multiple pipelines
func (p *Pipeline) addOutputToPipeline(newPipeline string, output string) error {
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
			return nil, fmt.Errorf("No pipeline registered for input %s", i)
		}
	}
	return pipelines, nil
}

func (p *Pipeline) checkIncomingVerticesForLoop(vertex *Vertex, loopDetect func(*Vertex, []string) error, visitedMap map[string]struct{}, path []string) error {
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

// recursively update the levels of all our incoming connections
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
	// update the weight of the from vertex if different
	if fromVertex.weight != weight {
		fromVertex.weight = weight
	}
	// ensure there are no loops
	loopDetect := func(v *Vertex, path []string) error {
		if v.name == to {
			return fmt.Errorf("Loop detected as vertex %s is already incoming vertex to %s on path %v", v.name, from, path)
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

	// update the level of from vertex to be the (max) level of our TO vertex + 1
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

	// connect the outputs to the inputs
	// we don't need this as edges are fetched using pipeline names mapped to the outputs
	// but this allows us to trace the edges using output field names as well (in case)
	// this is purely there for GetByField
	for _, o := range outputs {
		// check if there is a pipeline registered and connected for this output
		// if yes, connect our pipeline to the input fields
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

	// connect the pipelines to the inputs
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
	outgoingMap[v.name] = path{name: v.name, weight: v.weight, level: v.getLevel()}
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

// convert the connections map by vertex name and level into an array of pipelines sorted by level
func (p *Pipeline) connectionsToPipelinesByLevel(connections map[string]path) [][]string {
	// make them into path array so we can sort by level
	var ss []path
	for _, p := range connections {
		ss = append(ss, p)
	}
	sort.Slice(ss, func(i, j int) bool {
		// if levels are same, sort by weight
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

// get all the connected outputs from this pipeline and sort by level
func (p *Pipeline) Get(pipeline string) [][]string {
	p.Lock()
	defer p.Unlock()
	var connections = map[string]path{}
	if names, ok := p.pipelineMap[pipeline]; ok {
		for _, name := range names {
			outputConnections := p.getOutputs(name)
			if outputConnections != nil {
				// merge into connections
				// we save by pipeline names as they will be the same in case
				// we have multiple pipelines serving same output
				for k, v := range outputConnections {
					if pipelines, ok := p.pipelineMap[k]; ok {
						for _, name := range pipelines {
							connections[name] = v
						}
					}
				}
			}
		}
	}
	if len(connections) == 0 {
		return nil
	}
	return p.connectionsToPipelinesByLevel(connections)
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

// convert the pipelines sorted by levels into operations that can be used to execute the pipeline concurrently
func (p *Pipeline) PipelinesByLevelToOperations(pipelines [][]string) []PipelineOp {
	p.Lock()
	defer p.Unlock()
	pipelineOperations := []PipelineOp{}
	if pipelines == nil {
		return pipelineOperations
	}
	for level, pipeline := range pipelines {
		if level == 0 {
			// nothing to wait.
			for _, name := range pipeline {
				pipelineOperations = append(pipelineOperations, PipelineOp{Name: name})
			}
		} else {
			//build waitpipeline to wait on outgoing edges before executing pipeline
			multiplePipelineForSameOutput := map[string][]string{}
			for index, name := range pipeline {
				// check if there are multiple pipelines attached to same output
				// if yes, we ensure that the one with the highest weight/precedence is executed last to update the output
				if outputs, ok := p.pipelineOutputsMap[name]; ok {
					for _, o := range outputs {
						if outputPipelines, ok := p.pipelineMap[o]; ok && len(outputPipelines) > 1 {
							// add ourselves to the map of others
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
				// check same output multiple pipeline map for wait pipelines
				// we wait for lower precedence on same output so that the last update on output is from a higher precedence
				if index != 0 {
					if waitsForSameOutput, ok := multiplePipelineForSameOutput[name]; ok {
						for _, waitPipeline := range waitsForSameOutput {
							waitPipelines = append(waitPipelines, waitPipeline)
						}
					}
				}
				pipelineOperations = append(pipelineOperations, PipelineOp{Name: name, WaitPipelines: waitPipelines})
			}
		}
	}

	return pipelineOperations
}
