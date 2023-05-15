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
// cross-language transforms.
package expansionx

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
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
	cacheDir         = "~/.apache_beam/cache"
	jarCache         = cacheDir + "/jars"
)

func newJarGetter() *jarGetter {
	usr, _ := user.Current()
	cacheDir := filepath.Join(usr.HomeDir, jarCache[2:])
	return &jarGetter{repository: apacheRepository, groupID: beamGroupID, jarCache: cacheDir}
}

// GetDefaultRepositoryURL returns the current target URL for the defaultJarGetter,
// indicating what repository will be connected to when getting a Beam JAR.
func GetDefaultRepositoryURL() string {
	return defaultJarGetter.getRepositoryURL()
}

// SetDefaultRepositoryURL updates the target URL for the defaultJarGetter, changing
// which Maven repository will be connected to when getting a Beam JAR. Also
// validates that it has been passed a URL and returns an error if not.
//
// When changing the target repository, make sure that the value is the prefix
// up to "org/apache/beam" and that the organization of the repository matches
// that of the default from that point on to ensure that the conversion of the
// Gradle target to the JAR name is correct.
func SetDefaultRepositoryURL(repoURL string) error {
	return defaultJarGetter.setRepositoryURL(repoURL)
}

func buildJarName(artifactID, version string) string {
	return fmt.Sprintf("%s-%s.jar", artifactID, version)
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
		return "", fmt.Errorf("error in creating jar %s: %w", jarPath, err)
	}

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", fmt.Errorf("error in coping file %s inside jar %s: %w", file.Name(), jarPath, err)
	}

	return jarPath, nil
}

func extractJar(source, dest string) error {
	reader, err := zip.OpenReader(source)
	if err != nil {
		return fmt.Errorf("error opening jar for extractJar(%s,%s): %w", source, dest, err)
	}

	if err := os.MkdirAll(dest, 0700); err != nil {
		return fmt.Errorf("error creating directory %s in extractJar(%s,%s): %w", dest, source, dest, err)
	}

	for _, file := range reader.File {
		fileName := filepath.Join(dest, file.Name)
		if file.FileInfo().IsDir() {
			os.MkdirAll(fileName, 0700)
			continue
		}

		sf, err := file.Open()
		if err != nil {
			return fmt.Errorf("error opening source file %s: %w", file.Name, err)
		}
		defer sf.Close()

		df, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
		if err != nil {
			return fmt.Errorf("error opening destination file %s: %w", fileName, err)
		}
		defer df.Close()

		if _, err := io.Copy(df, sf); err != nil {
			return err
		}
	}
	return nil
}

func packJar(source, dest string) error {
	jar, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("error creating jar packJar(%s,%s)=%w", source, dest, err)
	}
	defer jar.Close()

	jarFile := zip.NewWriter(jar)
	defer jarFile.Close()

	fileInfo, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("source path %s doesn't exist: %w", source, err)
	}

	var sourceDir string
	if fileInfo.IsDir() {
		sourceDir = filepath.Base(source)
	}

	err = filepath.Walk(source, func(path string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error accesing path %s: %w", path, err)
		}
		fileHeader, err := zip.FileInfoHeader(fileInfo)
		if err != nil {
			return fmt.Errorf("error getting FileInfoHeader: %w", err)
		}

		if sourceDir != "" {
			fileHeader.Name = filepath.Join(sourceDir, strings.TrimPrefix(path, source))
		}

		if fileInfo.IsDir() {
			fileHeader.Name += "/"
		} else {
			fileHeader.Method = zip.Deflate
		}

		writer, err := jarFile.CreateHeader(fileHeader)
		if err != nil {
			return fmt.Errorf("error creating jarFile header: %w", err)
		}
		if fileInfo.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("error opening file %s: %w", path, err)
		}
		defer f.Close()

		if _, err = io.Copy(writer, f); err != nil {
			return fmt.Errorf("error copying file %s: %w", path, err)
		}
		return nil
	})
	return err
}

// MakeJar fetches additional classpath JARs and adds it to the classpath of
// main JAR file and compiles a fresh JAR.
func MakeJar(mainJar string, classpath string) (string, error) {
	usr, _ := user.Current()
	cacheDir := filepath.Join(usr.HomeDir, jarCache[2:])

	// fetch jars required in classpath
	classpaths := strings.Split(classpath, " ")
	classpathJars := []string{}
	for _, jar := range classpaths {
		path := expandJar(jar)
		if j, err := getLocalJar(path); err == nil {
			classpathJars = append(classpathJars, j)
		} else {
			return "", fmt.Errorf("error in getLocal(): %w", err)
		}
	}

	// classpath jars should have relative path
	relClasspath := []string{}
	for _, path := range classpathJars {
		relPath, err := filepath.Rel(cacheDir, path)
		if err != nil {
			return "", fmt.Errorf("error in creating relative path: %w", err)
		}
		relClasspath = append(relClasspath, relPath)
	}

	tmpDir := filepath.Join(cacheDir, "tmpDir")

	if err := extractJar(mainJar, tmpDir); err != nil {
		return "", fmt.Errorf("error in extractJar(): %w", err)
	}

	manifest, err := os.ReadFile(tmpDir + "/META-INF/MANIFEST.MF")
	if err != nil {
		return "", fmt.Errorf("error readingf: %w", err)
	}

	// trim the empty lines present at the end of MANIFEST.MF file.
	manifestLines := strings.Split(string(manifest), "\n")
	manifestLines = manifestLines[:len(manifestLines)-2]

	classpathString := fmt.Sprintf("%sClass-Path: %s\n", strings.Join(manifestLines, "\n"), strings.Join(relClasspath, " "))
	if err = os.WriteFile(tmpDir+"/META-INF/MANIFEST.MF", []byte(classpathString), 0660); err != nil {
		return "", fmt.Errorf("error writing ta manifest file: %w", err)
	}

	path, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("can't get current working directory: %w", err)
	}

	if err = os.Chdir(tmpDir); err != nil {
		return "", fmt.Errorf("can't change to temp directory %s for creating JAR: %w", tmpDir, err)
	}

	tmpJar := filepath.Join(cacheDir, "tmp.jar")
	if err = packJar(".", tmpJar); err != nil {
		return "", fmt.Errorf("error in packJar(): %w", err)
	}

	if err = os.Chdir(path); err != nil {
		return "", fmt.Errorf("can't change to old working directory %s: %w", path, err)
	}

	return tmpJar, nil
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

func getPythonVersion() (string, error) {
	for _, v := range []string{"python", "python3"} {
		cmd := exec.Command(v, "--version")
		if err := cmd.Run(); err == nil {
			return v, nil
		}
	}
	return "", fmt.Errorf("no python installation found")
}

// SetUpPythonEnvironment sets up the virtual ennvironment required for the
// Apache Beam Python SDK to run an expansion service module.
func SetUpPythonEnvironment(extraPackage string) (string, error) {
	py, err := getPythonVersion()
	if err != nil {
		return "", fmt.Errorf("no python installation found: %v", err)
	}
	extraPackages := []string{}
	if len(extraPackage) > 0 {
		extraPackages = strings.Split(extraPackage, " ")
	}

	// create python virtual environment
	sort.Strings(extraPackages)
	beamPackage := fmt.Sprintf("apache_beam[gcp,aws,azure,dataframe]==%s", core.SdkVersion)
	venvDir := filepath.Join(
		cacheDir, "venvs",
		fmt.Sprintf("py-%s-beam-%s-%s", py, core.SdkVersion, strings.Join(extraPackages, ";")),
	)
	venvPython := filepath.Join(venvDir, "bin", "python")

	if _, err := os.Stat(venvPython); err != nil {
		err := exec.Command(py, "-m", "venv", venvDir).Run()
		if err != nil {
			return "", errors.Wrap(err, "error creating new virtual environment for python expansion service")
		}
		err = exec.Command(venvPython, "-m", "pip", "install", "--upgrade", "pip").Run()
		if err != nil {
			return "", errors.Wrap(err, "error upgrading pip")
		}
		err = exec.Command(venvPython, "-m", "pip", "install", "--upgrade", "setuptools").Run()
		if err != nil {
			return "", errors.Wrap(err, "error upgrading setuptools")
		}
		err = exec.Command(venvPython, "-m", "pip", "install", beamPackage, "pyparsing==2.4.2").Run()
		if err != nil {
			return "", errors.Wrap(err, fmt.Sprintf("error installing beam package: %v", beamPackage))
		}
		if len(extraPackages) > 0 {
			cmd := []string{"-m", "pip", "install"}
			cmd = append(cmd, extraPackages...)
			err = exec.Command(venvPython, cmd...).Run()
			if err != nil {
				return "", errors.Wrap(err, "error installing dependencies")
			}
		}
		err = exec.Command(venvPython, "-c", "import apache_beam").Run()
		if err != nil {
			return "", errors.Wrap(err, "apache beam installation failed in virtualenv")
		}
	}
	return venvPython, nil
}
