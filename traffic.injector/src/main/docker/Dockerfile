FROM openjdk:8-jre
MAINTAINER Daniel Messer <daniel.messer@gmail.com>

RUN mkdir -p ${application.workdir}

WORKDIR ${application.workdir}
COPY ${application.dependencies} ${application.dependencies}
COPY ${project.build.finalName}.jar ${project.build.finalName}.jar
ENTRYPOINT ["/usr/bin/java", "-jar", "${project.build.finalName}.jar"]