all:
	/opt/gradle/gradle-6.4.1/bin/gradle wrapper
	./gradlew run --args="2 servers.txt 123456"
