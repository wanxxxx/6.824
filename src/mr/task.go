package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
)

//	type RPCTask interface {
//		DoMapTask(v ...interface{}) interface{}
//	}
const (
	NEW      = iota // Unassigned
	INIT     = iota // Assigned but not finished
	FINISHED = iota
)

//type MrTask struct {
//	Id            int
//	InputFilename string
//	//OutputFilePath string
//	Status int
//}

const (
	MAPTYPE    = 0
	REDUCETYPE = 1
)

type MrTask struct {
	TaskType int // map or reduce
	Id       int
	Filename string // map->input file, reduce->out file
	Status   int
}

//type MrTask struct {
//	Id             int
//	OutputFilePath string
//	Status         int
//}

// DoMapTask read input file, count the word and export the Key-value pair to file named "mr-x-y"
/**
 * The x means the id of map task, the y means the id of reduce task
 * The key-value pair may be stored to different files depend on the hash value of key
 */

func (task *MrTask) DoMapTask(mapf func(string, string) []KeyValue, nReduce int) error {
	// read the input file
	filename := task.Filename
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	// do map function and sort the result
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	/* Create tmp files to store data to different reduce partition,
	* the number of files is equal to the number of reduce partition */
	var tmpFiles []*os.File
	defer func() {
		for _, tmpFile := range tmpFiles {
			tmpFile.Close()
		}
	}()
	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile("", "mr-tmp-")
		if err != nil {
			return errors.New("info: failed to create temp tempFile for map task.\n\t" + err.Error())
		} else {
			tmpFiles = append(tmpFiles, tempFile)
		}
	}
	// save the key-value to json string
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(tmpFiles[reduceId])
		err = enc.Encode(&kv)
		if err != nil {
			return errors.New("info: failed to save key-value to tmp file.\n\t" + err.Error())
		}
	}
	// Rename the non-empty tmp file to save it
	for i, tmpFile := range tmpFiles {
		fileInfo, _ := tmpFile.Stat()
		if fileInfo.Size() > 0 {
			err = os.Rename(tmpFile.Name(), "mr-tmp-"+strconv.Itoa(task.Id)+"-"+strconv.Itoa(i))
			if err != nil {
				return errors.New("info: failed to rename tmp file.\n\t" + err.Error())
			}
		}
	}

	return nil
}

func (task *MrTask) DoReduceTask(reducef func(string, []string) string) error {
	inputFilenames, err := GetMatchPatternFileName("mr-tmp-\\d*-"+strconv.Itoa(task.Id)+"\\b", "./")
	if err != nil {
		return err
	}
	kva, err := Shuffle(inputFilenames)
	if err != nil {
		return err
	}
	ofile, err := os.Create(task.Filename)
	if err != nil {
		return err
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	defer ofile.Close()
	// clear tmp files
	for _, filename := range inputFilenames {
		os.Remove(filename)
	}
	return nil
}

func GetMatchPatternFileName(pattern string, dir string) ([]string, error) {
	var inputFiles []string
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.New("error: failed to open dir for reduce task")
	}
	for _, file := range files {
		matched, err := regexp.Match(pattern, []byte(file.Name()))
		if err != nil {
			log.Fatalf("error: failed to open file named %v", file)
		} else if matched {
			inputFiles = append(inputFiles, file.Name())
		}
	}
	return inputFiles, nil
}

// Shuffle combine all input files to a sorted key-vale arr
func Shuffle(files []string) ([]KeyValue, error) {
	var kva []KeyValue
	for _, filename := range files {
		inputFile, err := os.Open(filename)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		inputFile.Close()
	}
	sort.Sort(ByKey(kva))
	return kva, nil
}
