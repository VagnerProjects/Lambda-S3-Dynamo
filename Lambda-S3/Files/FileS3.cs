using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Lambda_S3.Files
{
    public static class FileS3
    {
        public static string GenerateValues()
        {
            var bairrosRand = new Random();

            var bairros = new List<string>()
            {
                "Jacarepágua",
                "Tijuca",
                "Maracanã",
                "Copacabana",
                "Ipanema",
                "Leme",
                "Lemblon",
                "Flamengo",
                "Botafogo",
                "Barra da Tijuca",
                "Recreio dos Bandeirantes",
                "Centro",
                "Freguesia de Jacarepágua",
                "Tanque",
                "Praça Seca",
                "Cordovil",
                "Penha",
                "Brás de Pina",
                "Taquara",
                "Vicente de Carvalho"
            };

            var index = bairrosRand.Next(0, 20);

            return bairros[index];
        }

        public static void PrintItemInFile(Dictionary<string, AttributeValue> attributeList, FileStream file)
        {
            foreach (KeyValuePair<string, AttributeValue> kvp in attributeList)
            {
                string attributeName = kvp.Key;
                AttributeValue value = kvp.Value;

                CreateFile(attributeName, value, file);
            }
        }
        private static void CreateFile(string attributeName, AttributeValue value, FileStream file)
        {
            var S = value.S == null ? "" : value.S;
            var N = value.N == null ? "" : value.N;
            var SS = value.SS == null ? "" : string.Join(",", value.SS.ToArray());
            var NS = value.NS == null ? "" : string.Join(",", value.NS.ToArray());

            var data = new UTF8Encoding(true).GetBytes($"Atributo: {attributeName}  Valor: {S + N + SS + NS} {Environment.NewLine}");
            file.Write(data, 0, data.Length);
        }
    }
}


