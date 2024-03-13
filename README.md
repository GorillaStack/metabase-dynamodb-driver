# metabase-dynamodb-driver
A metabase driver for Amazon DynamoDB

Forked from [kawasima/metabase-dynamodb-driver](https://github.com/kawasima/metabase-dynamodb-driver)

Updated to work with Metabase 0.48.0

### Pre-requisites for building the driver
- Install [Clojure CLI](https://clojure.org/guides/getting_started)
- Clone Metabase core repo `git clone https://github.com/metabase/metabase`

### Building the driver

```sh
# switch to the local checkout of the Metabase repo
cd /path/to/metabase/repo

# get absolute path to this driver directory
DRIVER_PATH=`readlink -f ~/dev/metabase-plugins/metabase-dynamodb-driver`

# Build driver. See explanation below
clojure \
  -Sdeps "{:aliases {:dynamodb {:extra-deps {com.metabase/dynamodb-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
  -X:build:dynamodb \
  build-drivers.build-driver/build-driver! \
  "{:driver :dynamodb, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}"
```

These build steps have been copied from the [Sample Metabase Driver: Sudoku](https://github.com/metabase/sudoku-driver). Check the [build section](https://github.com/metabase/sudoku-driver?tab=readme-ov-file#building-the-driver) of the REAMDE for more details.