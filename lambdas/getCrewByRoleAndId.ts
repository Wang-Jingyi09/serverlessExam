import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { MovieCrewRole } from "../shared/types";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
    DynamoDBDocumentClient,
    QueryCommand,
    QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

const ajv = new Ajv({ coerceTypes: true });
const isValidQueryParams = ajv.compile(
    schema.definitions["MovieCrewRole"] || {}
);

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
    try {
        const role = event.pathParameters?.role;
        const movieId = event.pathParameters?.movieId
            ? parseInt(event.pathParameters.movieId)
            : undefined;

        if (!role || !movieId) {
            return {
                statusCode: 400,
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ message: "Missing role or movie ID" }),
            };
        }

        const queryParams = { role, movieId };
        if (!isValidQueryParams(queryParams)) {
            return {
                statusCode: 400,
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ message: "Invalid query parameters", schema: schema.definitions["MovieCrewRole"] }),
            };
        }

        let commandInput: QueryCommandInput = {
            TableName: process.env.CREW_TABLE_NAME,
            KeyConditionExpression: "crewRole = :r and movieId = :mId",
            ExpressionAttributeValues: { ":r": role, ":mId": movieId },
        };

        const commandOutput = await ddbDocClient.send(new QueryCommand(commandInput));

        if (!commandOutput.Items || commandOutput.Items.length === 0) {
            return {
                statusCode: 404,
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ message: "No crew found for this role and movie ID" }),
            };
        }

        return {
            statusCode: 200,
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ data: commandOutput.Items }),
        };
    } catch (error) {
        console.error('Error: ', error);
        return {
            statusCode: 500,
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ error: "Internal server error" }),
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