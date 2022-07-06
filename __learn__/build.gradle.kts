plugins {
    base
    // check which dependencies need updating - including Gradle
    // https://github.com/ben-manes/gradle-versions-plugin
    id("com.github.ben-manes.versions") version "0.33.0"
    // top level licence enforcement analysis
    id("org.nosphere.apache.rat") version "0.7.0"
    // release management via gradle
    id("net.researchgate.release") version "2.8.1"
    id("org.apache.beam.module")
    id("org.sonarqube") version "3.0"
}

/*******/
// configure root project

tasks.rat {
	// set input directory to root instead of CWD, helps with .gitignore functioning as expected
	inputDir.set(project.rootDir)

	val exclusions = mutableListOf(
		// ignore files that we track but don't distribute
		"**/.github/**/*",
		"**/.gitkeep",
		"gradlew",
		"gradlew.bat",
		"gradle/wrapper/gradle-wrapper.properties",

		"**/package-list",
		"**/test.avsc",
		"**/user.avsc",
		"**/test/resources/**/*.txt",
		"**/test/resources/**/*.csv",
		"**/test/**/.placeholder",

		// default eclipse excludes neglect subprojects

		// proto/grpc generated wrappers
		"**/apache_beam/portability/api/**/*_pb2*.py",
		"**/go/pkg/beam/**/*.pb.go",

		// ignore go.sum files, which don't allow headers
		"**/go.sum",

		// ignore Go test data
		"**/go/data/**",

		// VCF test files
		"**/apache_beam/testing/data/vcf/*",

		// jdbc config files
		"**/META-INF/services/java.sql.Driver",

		// website build files
		"**/Gemfile.lock",
		"**/Rakefile",
		"**/.htaccess",
		"website/www/site/assets/scss/_bootstrap.scss",
		"website/www/site/assets/scss/bootstrap/**/*",
		"website/www/site/assets/js/**/*",
		"website/www/site/static/images/mascot/*.ai",
		"website/www/site/static/js/bootstrap*.js",
		"website/www/site/static/js/bootstrap/**/*",
		"website/www/site/themes",
		"website/www/yarn.lock",
		"website/www/package.json",
		"website/www/site/static/js/hero/lottie-light.min.js",
		"website/www/site/static/js/keen-slider.min.js",
		"website/www/site/assets/scss/_keen-slider.scss",
	)
}