# algo_processor
Author: Thomas Cole Varney

## Usage

### Database Setup
If running for this for the first time, first run the initialize_embedded_database.py script to create and set up the 
SQLite database that this system uses by calling  

```commandline
python initialize_embedded_database.py
```
in the command line

### Kafka and Zookeeper must be running and properly configured

The configuration files used in my implementation will be included in the /config folder that can be used to replicate 
the development environment

#### Starting Zookeeper and Kafka

Start the zookeeper server using 
```commandline
kafka-directory-on-your-machine/bin/zookeeper-server-start.sh config/zookeeper.properties
```

and start the kafka server with 
```commandline
kafka-directory-on-your-machine/bin/kafka-server-start.sh config/server.properties
```

Next, ensure that there is a topic created on your kafka server called 'transactions'

## Running the system

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)