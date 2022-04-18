package edu.sdsc.queryprocessing.scheduler.utils;

public enum ExecutorEnum {
    FilterDocuments, SplitDocuments, CreateDocuments, CollectCooccurance, KeywordsExtraction, GetNeighbor, Coreness, Corerank, AddNodeProperty,
    AggregateWeights, CollectGraphElementFromDocs, CollectGraphElementFromListOfPairs, CollectGraphElementFromRelation, ConstructTinkerpopGraph,
    ExecuteSQL, FetchSQLResult, SleepTest
}
