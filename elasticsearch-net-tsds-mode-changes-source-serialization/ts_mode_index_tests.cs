using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Cluster;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using FluentAssertions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace elasticsearch_net_tsds_mode_changes_source_serialization;

public class ts_mode_index_tests
{
    private static Guid id = Guid.NewGuid();
    private string componentMappingName = $"{id}-component-mapping";
    private string componentSettingName = $"{id}-component-setting";
    private string templateName = $"{id}-template";
    private string datastreamName = $"{id}-datastream";
    private const string elasticsearchAddress = "http://172.18.103.104:9201";

    private ElasticsearchClient client = null!;
    private ElasticsearchClientSettings settings = null!;

    public class SimpleDocument
    {
        public string Code { get; set; } = null!;
        [JsonPropertyName("@timestamp")]
        public DateTimeOffset Time { get; set; }
        public double Value { get; set; }
        public List<string> Tags { get; set; } = new List<string>();
    }

    [SetUp]
    public async Task Setup()
    {
        id = Guid.NewGuid();
        componentMappingName = $"{id}-component-mapping";
        componentSettingName = $"{id}-component-setting";
        templateName = $"{id}-template";
        datastreamName = $"{id}-datastream";

        settings = new ElasticsearchClientSettings(new Uri(elasticsearchAddress))
                .ThrowExceptions(true)
                .DisableDirectStreaming();

        client = new ElasticsearchClient(settings);

        PutComponentTemplateResponse putMappingReponse = await client.Cluster.PutComponentTemplateAsync<SimpleDocument>(
            componentMappingName,
            a => a
                .Template(t => t
                    .Mappings(m => m
                        .Properties(p => p
                            .Keyword(k => k.Code, c => c.TimeSeriesDimension())
                            .DoubleNumber(k => k.Value, c => c.TimeSeriesMetric(TimeSeriesMetricType.Gauge))
                            .DateNanos(k => k.Time, c => c.Format("strict_date_optional_time_nanos"))
                            .Wildcard(n => n.Tags)
                        )
                        .Dynamic(DynamicMapping.True)
                    )
                )
            );

        PutComponentTemplateResponse componentSettingsTemplateResponse
            = await client.Cluster.PutComponentTemplateAsync<SimpleDocument>(
                componentSettingName,
                d => d
                    .Template(tc => tc
                        .Settings(s => s
                            .Codec("best_compression")
                            .NumberOfShards(3)
                            .Index(i => i
                                .Mode("time_series")
                                .RoutingPath(new List<string> { "code" })
                            )
                        )
                    )
                );

        PutIndexTemplateResponse putTemplateReponse = await client.Indices.PutIndexTemplateAsync(
                templateName, rd => rd
                .ComposedOf(new List<Name> { componentMappingName, componentSettingName })
                .IndexPatterns(datastreamName)
                .DataStream(new DataStreamVisibility())
            );

        CreateDataStreamResponse createDatastreamResponse = await client.Indices.CreateDataStreamAsync(datastreamName);
    }

    [TestCase]
    public async Task can_insert_and_read_list_of_strings()
    {
        SimpleDocument doc = new SimpleDocument
        {
            Code = "1",
            Time = DateTimeOffset.UtcNow,
            Value = 1,
            Tags = new List<string> { "one_tag", "second_tag" }
        };

        await client.IndexAsync(doc, index: datastreamName);
        await client.Indices.RefreshAsync();

        var response = await client.SearchAsync<SimpleDocument>(datastreamName, r => r.Index(datastreamName)
            .Size(2)
            .Sort(s => s.Field(f => f.Time)));

        response.Documents.Should().HaveCount(1);
        response.Documents.First().Tags.Should().HaveCount(2);
    }

    [TestCase]
    public async Task can_insert_and_read_list_of_strings_with_a_single_element()
    {
        SimpleDocument doc = new SimpleDocument
        {
            Code = "2",
            Time = DateTimeOffset.UtcNow,
            Value = 2,
            Tags = new List<string> { "one_tag" }
        };

        await client.IndexAsync(doc, index: datastreamName);
        await client.Indices.RefreshAsync();

        var response = await client.SearchAsync<SimpleDocument>(datastreamName, r => r.Index(datastreamName)
            .Size(2)
            .Sort(s => s.Field(f => f.Time)));

        response.Documents.Should().HaveCount(1);
        response.Documents.First().Tags.Should().HaveCount(1);
    }
}
