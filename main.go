package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var parallel = flag.Int("p", 10, "parallel")
var mergeMode = flag.Bool("m", false, "merge as a single file")

type HDFS struct {
	hadoopHome string
	hadoopCmd  string
}

func NewHDFS() HDFS {
	hadoopHome := os.Getenv("HADOOP_HOME")
	return HDFS{
		hadoopHome: hadoopHome,
		hadoopCmd:  path.Join(hadoopHome, "bin", "hadoop"),
	}
}

func (s HDFS) Ls(inputPath string) []string {
	log.Printf("ls: %s\n", inputPath)
	output := s.Exec("fs", "-ls", inputPath)
	outputSplit := strings.Split(output, "\n")
	files := []string{}
	for _, line := range outputSplit {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "-") || line == "" {
			continue
		}
		split := strings.Fields(line)
		files = append(files, split[len(split)-1])
	}
	return files
}

func (s HDFS) Get(remote string, local string) {
	s.Exec("fs", "-get", remote, local)
}

func (s HDFS) Gets(remoteFiles []string, local string) {
	for _, f := range remoteFiles {
		s.Get(f, local)
	}
}

func (s HDFS) Exec(subCmd string, args ...string) string {
	jobConf := []string{}
	fullCmd := []string{
		subCmd,
	}
	for _, args := range [][]string{jobConf, args} {
		fullCmd = append(fullCmd, args...)
	}
	cmd := exec.Command(s.hadoopCmd, fullCmd...)
	stdout, _ := cmd.CombinedOutput()
	return string(stdout)
}

func downloadWithChan(hdfs HDFS, remoteFiles []string, localDir string, ch chan string) {
	for _, f := range remoteFiles {
		hdfs.Get(f, localDir)
		localBase := filepath.Base(f)
		ch <- path.Join(localDir, localBase)
	}
}

func chunk(lst []string, chunkNum int) (output [][]string) {
	chunkSize := int(math.Ceil(float64(len(lst)) / float64(chunkNum)))
	for i := 0; i < chunkNum; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if start >= len(lst) {
			break
		}
		if end > len(lst) {
			end = len(lst)
		}
		output = append(output, lst[start:end])
	}
	return output
}

func checkDir(path string) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("path is not a directory: %s", path)
	}
	return nil
}

func doMergeMode(hdfs HDFS, ch chan string, fileGroups [][]string, output string) {
	var wg sync.WaitGroup
	var tempDir, _ = ioutil.TempDir("", "pdoop")
	defer os.RemoveAll(tempDir)
	log.Println("tempDir: ", tempDir)
	for _, files := range fileGroups {
		wg.Add(1)
		go func(files []string) {
			defer wg.Done()
			downloadWithChan(hdfs, files, tempDir, ch)
		}(files)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	mergeFiles(ch, output)
}

// merge files from channel as a single files
func mergeFiles(ch chan string, output string) {
	out, err := os.Create(output)
	if err != nil {
		log.Fatal(err)
	}
	for f := range ch {
		in, err := os.Open(f)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := io.Copy(out, in); err != nil {
			log.Fatal(err)
		}
		err = in.Close()
		if err != nil {
			log.Fatal(err)
		}
		err = os.Remove(f)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = out.Close()
	if err != nil {
		log.Fatal(err)
	}
}

// put files into a directory
func doFilesMode(hdfs HDFS, ch chan string, fileGroups [][]string, outputDir string) {
	var wg sync.WaitGroup
	for _, files := range fileGroups {
		wg.Add(1)
		go func(files []string) {
			defer wg.Done()
			hdfs.Gets(files, outputDir)
		}(files)
	}
	wg.Wait()
	close(ch)
}

func main() {
	flag.Parse()
	if flag.NArg() != 2 {
		fmt.Println("Two arguments (input and output) are required!")
		flag.Usage()
		os.Exit(1)
	}
	input := flag.Arg(0)
	output := flag.Arg(1)
	log.Println("input: ", input)
	log.Println("output: ", output)
	if !*mergeMode {
		err := checkDir(output)
		if err != nil {
			log.Fatal(err)
		}
	}
	hdfs := NewHDFS()
	files := hdfs.Ls(input)
	slices := chunk(files, *parallel)
	ch := make(chan string, len(files))
	if *mergeMode {
		doMergeMode(hdfs, ch, slices, output)
	} else {
		outputDir := output
		doFilesMode(hdfs, ch, slices, outputDir)
	}
}
