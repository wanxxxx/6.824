package test

import (
	"6.824/mr"
	"fmt"
	"log"
	"plugin"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

var (
	reducef       func(key string, values []string) string
	mapf          func(filename string, contents string) []mr.KeyValue
	matchFiles    []string
	testKva       []mr.KeyValue
	nReduce       int
	testFilenames []string
	mapTask       mr.MapTask
	reduceTask    mr.ReduceTask
	c             *mr.Coordinator
)

func TestDoMapTask(t *testing.T) {
	GenerateTestData()
	mapTask.DoMapTask(mapf, nReduce)
}

func TestDoReduceTask(t *testing.T) {
	reduceTask.DoReduceTask(reducef)
}

func GenerateTestData() {
	matchFiles, _ = mr.GetMatchPatternFileName("pg-.*\\.txt", "../../main")
	c = mr.MakeCoordinator(matchFiles, 10)
	//c = mr.MakeCoordinator([]string{"pg-being_ernest.txt"}, 10)
	//c    = mr.MakeCoordinator([]string{"pg-being_ernest.txt", "pg-grimm.txt"}, 10)
	nReduce = 10
	mapf = func(filename string, contents string) []mr.KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []mr.KeyValue{}
		for _, w := range words {
			kv := mr.KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}
	reducef = func(key string, values []string) string {
		// return the number of occurrences of this word.
		return strconv.Itoa(len(values))
	}
	//testFilenames = []string{"mr-1-0", "mr-2-0"}
	//mapTask = mr.MapTask{0, "pg-1.txt", mr.INIT}
	//reduceTask = mr.ReduceTask{0, "mr-out-2", mr.INIT}

	//
	//testKva = append(testKva, mr.KeyValue{"a", "1"})
	//testKva = append(testKva, mr.KeyValue{"b", "1"})
	//testKva = append(testKva, mr.KeyValue{"c", "1"})
	//for _, filename := range testFilenames {
	//	file, _ := os.Create(filename)
	//	enc := json.NewEncoder(file)
	//	for _, kv := range testKva {
	//		enc.Encode(&kv)
	//	}
	//}
	//
	//file, _ := os.Create(mapTask.InputFilename)
	//file.Write([]byte("mapone maptwo mapthree"))

}
func TestGetMatchPatternFileName(t *testing.T) {
	GenerateTestData()
	dir := ""
	pattern := "mr-\\d-0"
	matchFiles, _ = mr.GetMatchPatternFileName(pattern, dir)
	sort.Strings(matchFiles)
	for i, matchFile := range matchFiles {
		if matchFile != testFilenames[i] {
			t.Error(matchFile)
		}
	}

	//t.Log("Pass: TestGetMatchPatternFileName")

}

func TestShuffle(t *testing.T) {
	GenerateTestData()
	kva, _ := mr.Shuffle(testFilenames)
	for _, kv := range kva {
		fmt.Println(kv.Key, kv.Value)
	}
}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
