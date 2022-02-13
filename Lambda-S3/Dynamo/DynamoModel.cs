using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Lambda_S3.Files;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Lambda_S3.Dynamo
{
    public class DynamoModel
    {
        public async Task<string> GetDynamo(ILambdaContext context, AmazonDynamoDBClient client)
        {
            context.Logger.Log("Montando objeto de requisição");

            var request = new QueryRequest
            {
                TableName = "Rio",
                ReturnConsumedCapacity = "TOTAL",
                KeyConditionExpression = "Cidade = :Cidade",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                   {
                     ":Cidade",
                     new AttributeValue
                     {
                       S = "Rio de Janeiro"
                     }
                   }
                }
            };

            context.Logger.Log("Realizando requisição no Dynamo");

            var response = await client.QueryAsync(request);

            context.Logger.Log("Iterando sobre itens e inserindo no arquivo");

            var filePath = "//tmp//Registros.txt";

            using (var file = File.OpenWrite(filePath))
            {
                foreach (Dictionary<string, AttributeValue> item in response.Items)
                {
                    FileS3.PrintItemInFile(item, file);
                }
            }

            context.Logger.Log("Retornando arquivo!");

            return filePath;
        }


        public async Task<PutItemResponse> PutItemTable(string tableName, ILambdaContext context, AmazonDynamoDBClient client)
        {

            context.Logger.Log("Objeto de requisição");

            var request = new PutItemRequest
            {
                TableName = tableName,
                Item = new Dictionary<string, AttributeValue>()
                {
                     { "Cidade", new AttributeValue { S = "Rio de Janeiro" }},
                     { "Bairro", new AttributeValue { S = FileS3.GenerateValues() }}
                }
            };

            context.Logger.Log("Inserindo dados na tabela do Dynamo");

            var response =  await client.PutItemAsync(request);

            context.Logger.Log("Tabela atualizada!");

            return response;
        }
    }

}

