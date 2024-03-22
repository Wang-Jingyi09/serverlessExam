import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand } from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {

        const parameters = event?.pathParameters;
        const movieId = parameters?.movieId ? parseInt(parameters.movieId) : undefined;

        const queryParams = event.queryStringParameters;
        const crewRole = queryParams?.crewRole ? queryParams.crewRole: undefined;


        let commandInput = {
            TableName: process.env.TABLE_NAME,
            KeyConditionExpression: "movieId = :movieId",
            ExpressionAttributeValues: {
                ":movieId": movieId,
            },
            ProjectionExpression: "#n, otherAttribute",
            ExpressionAttributeNames: {
                "#n": "names"
            }
        };

        // If crewRole is provided, modify the query to include it in the condition
        if (crewRole) {
            commandInput.KeyConditionExpression += " and crewRole = :crewRole";
            commandInput.ExpressionAttributeValues[":crewRole"] = crewRole;
        }

        const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));

        if (!commandOutput.Items || commandOutput.Items.length === 0) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({ message: "No crew found for the given criteria" }),
            };
        }

        const body = {
            data: commandOutput.Items.map(item => item.names), // Map to return only names
        };
        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify(body),
        };
    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ error }),
        };
    }
};

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
