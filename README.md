[![Community badge: Incubating](https://img.shields.io/badge/Lifecycle-Incubating-blue)](https://github.com/Camunda-Community-Hub/community/blob/main/extension-lifecycle.md#incubating-)
[![Community extension badge](https://img.shields.io/badge/Community%20Extension-An%20open%20source%20community%20maintained%20project-FF4700)](https://github.com/camunda-community-hub/community)

# maven-template

Empty maven project with defaults that incorporates Camunda Community Hub best practices.

## Usage

* Use this as a template for new Camunda Community Hub
  projects. (https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/creating-a-repository-from-a-template)
* Change names and URLs in `pom.xml`
  * `groupId`/`arrtifactId`
  ```
  <groupId>org.camunda.community.extension.name</groupId>
  <artifactId>give-me-a-name</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <packaging>jar</packaging>
  ```
  * URLs
  ```
  <scm>
    <url>https://github.com/camunda-community-hub/maven-template</url>
    <connection>scm:git:git@github.com:camunda-community-hub/maven-template.git</connection>
    <developerConnection>scm:git:git@github.com:camunda-community-hub/maven-tenmplate.git
    </developerConnection>
    <tag>HEAD</tag>
  </scm>
  ```
* Add contribution guide to the repo (
  e.g. [Contributing to this project](https://gist.github.com/jwulf/2c7f772570bfc8654b0a0a783a3f165e) )
* Select desired license and exchange `LICENSE` file

## Features

- IDE integration
  - https://editorconfig.org/
- GitHub Integration
  - Dependabot enabled for Maven dependencies
  - Backport action (https://github.com/zeebe-io/backport-action)
- Maven POM
  - Release to Maven, Nexus and GitHub
  - Google Code Formatter
  - JUnit 5
  - AssertJ
  - Surefire Plugin
  - JaCoCo Plugin (test coverage)
  - flaky test extractor (https://github.com/zeebe-io/flaky-test-extractor-maven-plugin)

## Versions

Different versions are represented in different branches

- `main` - Java 11

