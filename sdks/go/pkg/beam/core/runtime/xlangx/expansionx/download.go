// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package expansionx contains utilities for starting expansion services for
// cross-language transforms. All code in this package is currently experimental.
package expansionx

import (
	"archive/zip"
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

type url string

type jarGetter struct {
	repository url
	groupID    string
	jarCache   string
}

var defaultJarGetter = newJarGetter()

const (
	apacheRepository = url("https://repo.maven.apache.org/maven2")
	beamGroupID      = "org/apache/beam"
	jarCache         = "~/.apache_beam/cache/jars"
)

func newJarGetter() *jarGetter {
	usr, _ := user.Current()
	cacheDir := filepath.Join(usr.HomeDir, jarCache[2:])
	return &jarGetter{repository: apacheRepository, groupID: beamGroupID, jarCache: cacheDir}
}

// GetRepositoryURL returns the current target URL for the defaultJarGetter,
// indicating what repository will be connected to when getting a Beam JAR.
func GetDefaultRepositoryURL() string {
	return defaultJarGetter.getRepositoryURL()
}

// SetRepositoryURL updates the target URL for the defaultJarGetter, changing
// which Maven repository will be connected to when getting a Beam JAR. Also
// validates that it has been passed a URL and returns an error if not.
//
// When changing the target repository, make sure that the value is the prefix
// up to "org/apache/beam" and that the organization of the repository matchest
// that of the default from that point on to ensure that the conversion of the
// Gradle target to the JAR name is correct.
func SetDefaultRepositoryURL(repoURL string) error {
	return defaultJarGetter.setRepositoryURL(repoURL)
}

func buildJarName(artifactId, version string) string {
	return fmt.Sprintf("%s-%s.jar", artifactId, version)
}

func getMavenJar(artifactID, groupID, version string) string {
	return strings.Join([]string{string(apacheRepository), strings.ReplaceAll(groupID, ".", "/"), artifactID, version, buildJarName(artifactID, version)}, "/")
}

func expandJar(jar string) string {
	if jarExists(jar) {
		return jar
	} else if strings.HasPrefix(jar, "http://") || strings.HasPrefix(jar, "https://") {
		return jar
	} else {
		components := strings.Split(jar, ":")
		groupID, artifactID, version := components[0], components[1], components[2]
		path := getMavenJar(artifactID, groupID, version)
		return path
	}
}

func getLocalJar(url string) (string, error) {

	jarName := path.Base(url)
	usr, _ := user.Current()
	cacheDir := filepath.Join(usr.HomeDir, jarCache[2:])
	jarPath := filepath.Join(cacheDir, jarName)

	if jarExists(jarPath) {
		return jarPath, nil
	}

	resp, err := http.Get(string(url))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to connect to %v: received non 200 response code, got %v", url, resp.StatusCode)
	}

	file, err := os.Create(jarPath)
	if err != nil {
		return "", errors.New(fmt.Sprintf("error in create: %v", err))
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", errors.New(fmt.Sprintf("error in copy: %v", err))
	}

	return jarPath, nil
}

func AddClasspathJars(mainJar string, classpath []string) (string, error) {
	usr, _ := user.Current()
	cacheDir := filepath.Join(usr.HomeDir, jarCache[2:])
	classpathJars := []string{}
	for _, jar := range classpath {
		path := expandJar(jar)
		if j, err := getLocalJar(path); err == nil {
			classpathJars = append(classpathJars, j)
		} else {
			return "", errors.New(fmt.Sprintf("error in getLocal(): %v", err))
		}
	}

	compositeJarDir := filepath.Join(cacheDir, "composite-jar")
	err := os.MkdirAll(compositeJarDir, 0700)
	if err != nil {
		return "", err
	}
	relClasspath := []string{}

	for _, pattern := range append([]string{mainJar}, classpathJars...) {
		path, err := filepath.Abs(pattern)
		if err != nil {
			return "", err
		}
		relPath, err := filepath.Rel(compositeJarDir, path)
		if err != nil {
			return "", fmt.Errorf("error in creating relative path: %v", err)
		}
		relClasspath = append(relClasspath, relPath)
		if _, err = os.Stat(filepath.Join(compositeJarDir, relPath)); errors.Is(err, os.ErrNotExist) {
			os.Symlink(path, filepath.Join(compositeJarDir, relPath))
		}
	}
	sort.Strings(relClasspath)
	// compositeJar := filepath.Join(compositeJarDir, strings.Join(relClasspath, " "))
	compositeJar := filepath.Join(compositeJarDir, "composite.jar")
	if !jarExists(compositeJar) {
		archive, err := zip.OpenReader(mainJar)
		if err != nil {
			return "", fmt.Errorf("error in OpenReader(): %v", err)
		}
		defer archive.Close()
		manifest, err := archive.Open("META-INF/MANIFEST.MF")
		if err != nil {
			return "", err
		}
		defer manifest.Close()

		scanner := bufio.NewScanner(manifest)
		mainClass := ""
		for scanner.Scan() {
			if scan := scanner.Text(); strings.HasPrefix(scan, "Main-Class:") {
				mainClass = scan
				break
			}
		}
		tmpJar := fmt.Sprintf("%s.tmp", compositeJar)
		tmpArchive, err := os.Create(tmpJar)
		if err != nil {
			return "", err
		}
		defer tmpArchive.Close()

		zipf := zip.NewWriter(tmpArchive)
		defer zipf.Close()
		mfile, err := zipf.Create("META-INF/MANIFEST.MF")
		if err != nil {
			return "", err
		}
		writeBuf := []byte(fmt.Sprintf("Manifest-Version: 1.0\n%s\nClass-Path: %s\n", mainClass, strings.Join(relClasspath, " ")))
		mfile.Write(writeBuf)
		err = os.Rename(tmpJar, compositeJar)
		if err != nil {
			return "", nil
		}
	}
	return compositeJar, nil
}

// GetBeamJar checks a temporary directory for the desired Beam JAR, downloads the
// appropriate JAR from Maven if not present, then returns the file path to the
// JAR.
func GetBeamJar(gradleTarget, version string) (string, error) {
	return defaultJarGetter.getJar(gradleTarget, version)
}

func (j *jarGetter) getRepositoryURL() string {
	return string(j.repository)
}

func (j *jarGetter) setRepositoryURL(repoURL string) error {
	if !strings.HasPrefix(repoURL, "http") {
		return fmt.Errorf("repo URL %v does not have an http or https prefix", repoURL)
	}
	j.repository = url(strings.TrimSuffix(repoURL, "/"))
	return nil
}

func (j *jarGetter) getJar(gradleTarget, version string) (string, error) {
	strippedTarget := dropEndOfGradleTarget(gradleTarget)
	fullURL, jarName := j.getURLForBeamJar(strippedTarget, version)

	err := os.MkdirAll(j.jarCache, 0700)
	if err != nil {
		return "", err
	}

	jarPath := filepath.Join(j.jarCache, jarName)

	if jarExists(jarPath) {
		return jarPath, nil
	}

	if strings.Contains(version, ".dev") {
		return "", fmt.Errorf("cannot pull dev versions of JARs, please run \"gradlew %v\" to start your expansion service",
			gradleTarget)
	}

	resp, err := http.Get(string(fullURL))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("failed to connect to %v: received non 200 response code, got %v", fullURL, resp.StatusCode)
	}

	file, err := os.Create(jarPath)
	if err != nil {
		return "", err
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", err
	}

	return jarPath, nil
}

// getURLForBeamJar builds the Maven URL for the JAR and the JAR name, returning both
// separately so the JAR name can be used for saving the file later.
func (j *jarGetter) getURLForBeamJar(gradleTarget, version string) (url, string) {
	gradlePath := strings.ReplaceAll(gradleTarget, ":", "-")
	targetPath := "beam" + gradlePath
	jarName := fmt.Sprintf("%s-%s.jar", targetPath, version)
	finalURL := j.repository + url(path.Join("/", j.groupID, targetPath, version, jarName))
	return finalURL, jarName
}

// dropEndOfGradleTarget drops the last substring off of the gradle target. This
// is used to build the Maven target and JAR name (the last substring on the gradle)
// command is usually a directive, not a reference to the desired JAR.)
func dropEndOfGradleTarget(gradleTarget string) string {
	i := strings.LastIndex(gradleTarget, ":")
	return gradleTarget[:i]
}

// jarExists checks if a file path exists/is accessible and returns true if os.Stat
// does not return an error. Does not create a file or directory if not present.
func jarExists(jarPath string) bool {
	_, err := os.Stat(jarPath)
	return err == nil
}
