all:
	gradle wrapper
	./gradlew run --args="servers.txt"