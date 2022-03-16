#!/bin/bash
# Run script in a docker container

function usage {
    echo "Usage: $0 (-b) [-i]"
    echo "  -b     Run Bash, leave shell open."
    exit 1
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

SCRIPT_NAME="ingest-on-demand"
DOCKER_IMAGE="${SCRIPT_NAME}"

# path of this/self script.
CURR_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPT_PATH="$CURR_PATH"

TIMEOUT="${TIMEOUT:-3600}"

CMD_BASH="/bin/bash"

INTERACTIVE=""
while getopts ":qbeif" opt; do
    case $opt in
        b)
            CMD=${CMD_BASH}
            INTERACTIVE="--interactive"
            echo "Skipping environment var checks."
            ;;
        i)
            INTERACTIVE="--interactive"
            ;;
        p)
            PRIVILEGED="--privileged"
            ;;
    esac
done

if [ ! -z "$INTERACTIVE" ]; then
    echo "Interactive Docker shell."
else
    echo "Non-interactive Docker shell."
fi

echo ""
echo "HOSTNAME: $(/bin/hostname)"
echo -e "\nDisk:\n$(/bin/df -h)"
IP="$(/sbin/ifconfig | perl -nle 's/dr:(\S+)/print $1/e')"
echo -e "\nIP:\n$IP"

# build docker image
echo "Building docker container.  Please wait ..."
docker build -t $DOCKER_IMAGE:latest "$CURR_PATH"

if [ $? -ne 0 ]; then
    echo "WARNING: Docker build failed.  Check Docker build logs.  Exiting."
    exit -1
fi

echo "Script directory: $SCRIPT_PATH"
echo "Running as: $(id)"
echo "Running: $CMD"

# run docker container, pass env vars
docker run ${PRIVILEGED} --rm --tty ${INTERACTIVE} \
    -e TIMEOUT="${TIMEOUT}" \
    -e INTERACTIVE="${INTERACTIVE}" \
    --volume ingest-on-demand \
    $DOCKER_IMAGE:latest $CMD 2>&1
