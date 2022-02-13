using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using Lambda_S3.Files;
using Lambda_S3.Dynamo;
using Microsoft.AspNetCore.Http;
using Amazon.S3.Transfer;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace Lambda_S3
{
    public class Function
    {
        /// <summary>
        /// Função que é executada através de uma trigger para enviar um objeto ao AmazonS3
        /// </summary>
        /// <param name="input">parametro não obrigatório de entrada</param>
        /// <param name="context">Contexto de evento da lambda</param>
        /// <returns></returns>
        public async Task FunctionHandler(object input, ILambdaContext context)
        {
            context.Logger.Log("Iniciando execução...");

            var task = Task.Run(() => CreateObjectS3(context));

            await Task.WhenAny(task);

            context.Logger.Log("Finalizando execução...");
        }

        private async Task CreateObjectS3(ILambdaContext context)
        {
            try
            {

                context.Logger.Log("Criando cliente do S3...");
                context.Logger.Log("Criando cliente do DynamoDB...");

                var client = new AmazonS3Client(Amazon.RegionEndpoint.SAEast1);
                var clientDynamo = new AmazonDynamoDBClient(Amazon.RegionEndpoint.SAEast1);

                var dynamoModel = new DynamoModel();

                context.Logger.Log("Inserindo valores no dynamo");

                var dynamoResponse = await dynamoModel.PutItemTable("Rio", context, clientDynamo);

                context.Logger.Log($"Response Dynamo: {dynamoResponse.HttpStatusCode}");

                context.Logger.Log("Recuperando dados do Dynamo e arquivo");

                var fileDynamoResponse = await dynamoModel.GetDynamo(context, clientDynamo);

                context.Logger.Log("Criando objeto de transferência de dados para o S3");

                var fileTransferUtility = new TransferUtility(client);

                context.Logger.Log("Recuperando Objetos atuais do S3");

                await GetObjectsBucket(client, context);

                context.Logger.Log("Enviando objeto para o AmazonS3");

                fileTransferUtility.Upload(fileDynamoResponse, "bucket-name");

                context.Logger.Log("Objeto salvo com sucesso!");

            }
            catch (Exception ex)
            {
                context.Logger.Log($"Houve um erro ao executar a função: {ex.Message}");
            }
        }

        private async Task GetObjectsBucket(AmazonS3Client amazonS3, ILambdaContext context)
        {
            context.Logger.Log("Recuperando objetos do bucket: bucket-name");

            var objects = await amazonS3.ListObjectsAsync("bucket-name");

            context.Logger.Log("Iterando sobre Lista de objetos e retornando para CloudWatch");

            int qtd = 0;

            foreach (var objectBucket in objects.S3Objects)
            {
                context.Logger.Log(objectBucket.Key);
                qtd++;
            }

            context.Logger.Log($"Quantidade de objetos no bucket: {qtd}");
        }
    }
}
