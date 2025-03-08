import { Handler } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDocumentClient();

export const handler: Handler = async (event) => {
  try {
    console.log("Event: ", JSON.stringify(event));

    const queryParams = event?.queryStringParameters;
    if (!queryParams?.movieId) {
      return {
        statusCode: 400,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Missing movieId parameter" }),
      };
    }

    const movieId = parseInt(queryParams.movieId);
    if (isNaN(movieId)) {
      return {
        statusCode: 400,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: "Invalid movieId parameter" }),
      };
    }

    let commandInput: QueryCommandInput = {
      TableName: process.env.CAST_TABLE_NAME,
      KeyConditionExpression: "movieId = :m",
      ExpressionAttributeValues: { ":m": movieId },
    };

    if ("roleName" in queryParams) {
      commandInput = {
        ...commandInput,
        IndexName: "roleIx", // Ensure this index exists in DynamoDB
        KeyConditionExpression: "movieId = :m and begins_with(roleName, :r)",
        ExpressionAttributeValues: { ":m": movieId, ":r": queryParams.roleName },
      };
    } else if ("actorName" in queryParams) {
      commandInput = {
        ...commandInput,
        KeyConditionExpression: "movieId = :m and begins_with(actorName, :a)",
        ExpressionAttributeValues: { ":m": movieId, ":a": queryParams.actorName },
      };
    }

    // Query DynamoDB for cast data
    const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));
    
    let responseBody: any = { data: commandOutput.Items };

    // If 'movie=true' is provided, fetch movie metadata
    if (queryParams.movie === "true") {
      const movieData = await getMovieMetadata(movieId);

      responseBody.movie = movieData;
    }

    return {
      statusCode: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(responseBody),
    };
  } catch (error: any) {
    console.error("Error:", error.message || JSON.stringify(error));
    return {
      statusCode: 500,
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ error: error.message || "Internal Server Error" }),
    };
  }
};

// Function to fetch movie metadata (title, genre ids, overview)
async function getMovieMetadata(movieId: number) {
  // Assuming there's a DynamoDB table or another source where movie metadata is stored
  const movieMetadata = await ddbDocClient.send(
    new QueryCommand({
      TableName: process.env.MOVIE_TABLE_NAME,
      KeyConditionExpression: "movieId = :m",
      ExpressionAttributeValues: { ":m": movieId },
    })
  );

  if (movieMetadata.Items && movieMetadata.Items.length > 0) {
    return movieMetadata.Items[0]; // Returning the first movie metadata
  } else {
    return {
      title: "Unknown Title",
      genreIds: [],
      overview: "No overview available",
    };
  }
}

function createDocumentClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  return DynamoDBDocumentClient.from(ddbClient, {
    marshallOptions: {
      convertEmptyValues: true,
      removeUndefinedValues: true,
      convertClassInstanceToMap: true,
    },
    unmarshallOptions: { wrapNumbers: false },
  });
}
