package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
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

func doMergeMode(hdfs HDFS, files []string, outputFile string) error {
	var tempDir, _ = ioutil.TempDir("", "pdoop")
	defer os.RemoveAll(tempDir)
	log.Println("tempDir: ", tempDir)
	filesCh := make(chan string)
	resultsCh := make(chan string, len(outputFile))
	defer close(resultsCh)
	// feed files
	go func() {
		for _, f := range files {
			filesCh <- f
		}
		close(filesCh)
	}()
	// parallel working on it and generate results
	for i := 0; i < *parallel; i++ {
		go func() {
			for f := range filesCh {
				hdfs.Get(f, tempDir)
				baseName := filepath.Base(f)
				resultsCh <- path.Join(tempDir, baseName)
			}
		}()
	}
	err := mergeFiles(resultsCh, len(files), outputFile)
	return err
}

// merge files from channel as a single files
func mergeFiles(resultsCh <-chan string, count int, output string) error {
	out, err := os.Create(output)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < count; i++ {
		f := <-resultsCh
		in, err := os.Open(f)
		if err != nil {
			log.Fatal(err)
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			log.Fatal(err)
			return err
		}
		err = in.Close()
		if err != nil {
			log.Fatal(err)
			return err
		}
		err = os.Remove(f)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	err = out.Close()
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// put files into a directory
func doFilesMode(hdfs HDFS, files []string, outputDir string) error {
	filesCh := make(chan string)
	resultsCh := make(chan struct{})
	defer close(resultsCh)
	go func() {
		for _, f := range files {
			filesCh <- f
		}
		close(filesCh)
	}()
	for i := 0; i < *parallel; i++ {
		go func() {
			for f := range filesCh {
				hdfs.Get(f, outputDir)
				resultsCh <- struct{}{}
			}
		}()
	}
	for range files {
		<-resultsCh
	}
	return nil
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
	if *mergeMode {
		doMergeMode(hdfs, files, output)
	} else {
		doFilesMode(hdfs, files, output)
	}
}
