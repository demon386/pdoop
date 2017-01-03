package main

import (
	"flag"
	"fmt"
	"github.com/demon386/hdfs"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
)

var parallel = flag.Int("p", 10, "parallel")
var mergeMode = flag.Bool("m", false, "merge as a single file")

func downloadWithChan(hdfs hdfs.HDFS, remoteFiles []string, localDir string, ch chan string) {
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

func doMergeMode(hdfs hdfs.HDFS, files []string, outputFile string) error {
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
func doFilesMode(hdfs hdfs.HDFS, files []string, outputDir string) error {
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
	} else {
		if _, err := os.Stat(output); err == nil {
			log.Fatalf("target file %s already exists\n", output)
		}
	}
	hdfs := hdfs.NewHDFS()
	files := hdfs.Ls(input)
	if *mergeMode {
		doMergeMode(hdfs, files, output)
	} else {
		doFilesMode(hdfs, files, output)
	}
}
