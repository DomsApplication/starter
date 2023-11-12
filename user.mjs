import {GetItemCommand,QueryCommand,PutItemCommand,DeleteItemCommand,UpdateItemCommand} from "@aws-sdk/client-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {marshall ,unmarshall } from "@aws-sdk/util-dynamodb";


const ddbClient = new DynamoDBClient();
const tableName = 'POC';

export const handler = async (event) => {
  try {
    
  let body;
  // TODO implement
  
    switch (event.httpMethod) {
      case "GET":
        if (event.pathParameters != null) {
          body = await getUser(event.pathParameters.id);
        } else {
          body = await getAllUser();
        }
        break;
        case "POST":
          body  = await createUser(event);
        break;
        case "DELETE":
          body = await deleteUser(event.pathParameters.id);
        break;
        case "PUT":
          body = await updateUser(event);
        break;
        
      default:
        return {
          statusCode: 400,
          headers: { "Content-Type": "text/plain" },
          body: "Invalid HTTP method",
        };
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json" }, // Update content type to JSON
      body: JSON.stringify(body), // Convert body to JSON string
    };
  } 
  catch (error) {
    console.error(error);
    return {
      statusCode: 500,
      headers: { "Content-Type": "text/plain" },
      body: "Internal Server Error",
    };
  }
};
  

const getUser = async(userId) =>{
  console.log("getUser");
  try{
    let userIdKey=`U#${userId}`;
    const params=  {
    "TableName": "POC",
    "Key": {
      "PK": {
        "S": userIdKey
      },
      "SK": {
        "S": userIdKey
      }
    }
  };
    
     const response = await ddbClient.send(new GetItemCommand(params));
    console.log("User data:", JSON.stringify(response.Item, null, 2)); // Log and print user data
    return response.Item;
  }
  catch (error){
    console.error(error);
    throw error;
  }
};


const getAllUser = async() => {
  console.log("getAllUser");
 try{
  const params = {
    "TableName": "POC",
    "ScanIndexForward": true,
    "IndexName": "ENTITIES_INX",
    "KeyConditionExpression": "#c9460 = :c9460",
    "ExpressionAttributeValues": {
      ":c9460": {
        "S": "USER"
      }
    },
    "ExpressionAttributeNames": {
      "#c9460": "ENTITIES"
    }
  };
 

 const response = await ddbClient.send(new QueryCommand(params));
    console.log("DynamoDB Query Response:", JSON.stringify(response, null, 2));

    // Return the Items from the response, not response.Item
    return response.Items || [];
  
 }
  
  catch(e){
    console.error(e);
    throw e;
  }
  
  
};


const createUser = async(event) =>{
  console.log(`createEntity function. event : "${event}"`);

    try {
        const body = JSON.parse(event.body);
            const userItem = {
                PK: `U#${body.id}`,
                SK: `U#${body.id}`,
                ENTITIES: "USER",
                PAYLOAD: JSON.stringify(body),
            };

            const params = {
                TableName: tableName,
                Item: marshall(userItem),
            };

            await ddbClient.send(new PutItemCommand(params));

            return {
                statusCode: 200,
                body: JSON.stringify({ message: 'User item created successfully' })
            };
            
}
      catch (error) {
        return {
            statusCode: 500,
            body: JSON.stringify({ error: error.message })
        };
    }
};


const deleteUser = async(userId) =>{
  try{
    let userIdKey=`U#${userId}`;
    const params=  {
    "TableName": "POC",
    "Key": {
      "PK": {
        "S": userIdKey
      },
      "SK": {
        "S": userIdKey
      }
    }
  };
     await ddbClient.send(new DeleteItemCommand(params));
    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'User deleted successfully' })
  };
  }
  catch(error){
    console.error(error);
    throw error;
  }
 
};



const updateUser = async(event) =>{
     try {
    
 const params = {
      "TableName": "POC",
    "Key": {
      "PK": {
        "S": "U#kumar@as.com"
      },
      "SK": {
        "S": "U#kumar@as.com"
      }
    },
    "UpdateExpression": "SET #b4090 = if_not_exists(#b4091,:b4090)",
    "ExpressionAttributeValues": {
      ":b4090": {
        "M": {
          "id": {
            "S": "kumara@as.com"
          },
          "firstName": {
            "S": "kumara"
          }
        }
      }
    },
    "ExpressionAttributeNames": {
      "#b4090": "PAYLOAD",
      "#b4091": "PAYLOAD"
    }
  };

    await ddbClient.send(new UpdateItemCommand(params));
    
    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'User updated successfully' }),
    };
  } catch (error) {
    console.error("Error updating user:", error);
    
    return {
      statusCode: 500,
      body: JSON.stringify({ message: 'Internal Server Error' }),
    };
  }
};
