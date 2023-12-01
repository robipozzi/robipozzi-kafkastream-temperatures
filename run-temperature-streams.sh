source ./setenv.sh $1 $2

##### Variable section - START
SCRIPT=run-temperature-streams.sh
DEFAULT_TRUSTSTORE=$HOME/opt/robipozzi-kafka/ssl/kafka.client.truststore.jks
JAR_NAME=robipozzi-kafkastreams-temperatures
VERSION=0.0.1-SNAPSHOT
##### Variable section - END

##### Function section - START
main()
{
	CURRENT_DIRECTORY=$(pwd)
	if [ -z $PROFILE_OPTION ]; then 
        printProfile
    fi
	java -version
	
	#############################################################################################################################################
	# The application will be launched with the profile selected (i.e.: --spring.profiles.active=<PROFILE>), 									#
	# Spring Boot will search for a configuration file named application-<PROFILE>.properties and will load configuration properties from that. #
	# 																																			#
	# Alternatively, a different configuration file can be used at runtime by setting the following environment property:						#
	# --spring.config.location=file://<path to application config file>																			#
	#############################################################################################################################################
	case $PROFILE_OPTION in
		1)  java -jar target/$JAR_NAME-$VERSION.jar
			;;
		2)  inputTruststore
			inputTruststorePassword
			export TRUSTSTORE_LOCATION=$TRUSTSTORE
			export TRUSTSTORE_PASSWORD=$TRUSTSTORE_PWD
			java -jar target/$JAR_NAME-$VERSION.jar --spring.profiles.active=ssl
			;;
        3)  java -jar target/$JAR_NAME-$VERSION.jar --spring.profiles.active=confluent
            ;;
		*) 	printf "\n${red}No valid option selected${end}\n"
			printProfile
			;;
	esac
}

inputTruststore()
{
    ###### Set truststore location
    if [ "$TRUSTSTORE" != "" ]; then
        echo Truststore location is set to $TRUSTSTORE
    else
        echo ${grn}Enter truststore location - leaving blank will set truststore to ${end}${mag}$DEFAULT_TRUSTSTORE : ${end}
        read TRUSTSTORE
        if [ "$TRUSTSTORE" == "" ]; then
        	TRUSTSTORE=$DEFAULT_TRUSTSTORE
        fi
    fi
}
##### Function section - END

# ##############################################
# #################### MAIN ####################
# ##############################################
# ************ START evaluate args ************"
if [ "$1" != "" ]; then
    setProfile
fi
# ************** END evaluate args **************"
main