all:
	/opt/gradle/gradle-6.4.1/bin/gradle wrapper
	./gradlew run --args="servers.txt"
