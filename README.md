# Databricks RAG Question-Answerer

## Overview

This project implements a Retrieval-Augmented Generation (RAG) question-answering system using Azure Databricks. It combines semantic search with a Large Language Model (LLM) to generate answers grounded in retrieved document content.

The system uses the SQuAD 2.0 dataset to build a document corpus, processes and chunks the source text, generates embeddings, and indexes the resulting chunks using Databricks AI Search.

Users can submit questions that are answered using retrieved context (RAG) or directly by the LLM without retrieval. Both approaches are logged and can be compared to evaluate the effectiveness of retrieval-augmented generation.

## Dataset

**SQuAD 2.0 (Stanford Question Answering Dataset)**

Source: https://huggingface.co/datasets/rajpurkar/squad_v2

## Architecture

The project follows a simplified Medallion Architecture for data preparation, followed by an embedding, retrieval, and generation workflow.

### 1. Data Ingestion and Preparation

Raw SQuAD 2.0 training and validation data is loaded into Bronze Delta tables.

The data is subsequently cleaned and transformed into Silver tables:
- HTML tags and unnecessary whitespace are removed.
- Empty text fields are handled.
- Answer text and answer positions are extracted.
- Unanswerable questions are identified.
- Duplicate records are removed.

The cleaned contexts are then combined into a document corpus. Duplicate documents are removed, and each document receives a unique identifier.

### 2. Document Chunking

Documents are split into smaller, overlapping text chunks to improve retrieval granularity.

Chunking configuration:
- Chunk size: 120 words.
- Chunk overlap: 20 words.
- Chunk step: 100 words.

### 3. Embeddings and AI Search

Databricks AI Search is used to provide semantic retrieval capabilities.

The `chunk_text` column is used as the embedding source. Databricks generates embeddings using its configured embedding model and maintains a vector index synchronized with the underlying Delta table.

The index supports similarity searches that retrieve the most relevant chunks for a given question.

### 4. RAG Question-Answering Workflow

The question-answering workflow consists of the following steps:

1. A user submits a question.
2. AI Search retrieves the top 5 most relevant document chunks.
3. Retrieved chunks are combined into a context prompt.
4. The question and context are sent to a chat LLM.
5. The LLM generates an answer based on the supplied context.
6. The answer and retrieved source metadata are returned.

The system prompt instructs the model to use only the supplied context and explicitly acknowledge when insufficient information is available.

### 5. Baseline Without Retrieval

A second question-answering workflow is implemented without AI Search.

In this approach, the same chat LLM receives the user's question directly and generates an answer using its existing knowledge, without access to retrieved document chunks.

This provides a baseline for measuring the contribution of retrieval to answer quality and reliability.

## Logging and Monitoring

A dedicated Delta table stores information about question-answering requests.

Logged fields include:
- Request ID and timestamp.
- Original question.
- Prompt sent to the LLM.
- Generated response.
- Retrieved chunk IDs and document titles.
- Number of retrieved chunks.
- Model name.
- Total request latency.
- Retrieval and generation latency.
- Input and output token usage.
- Generation finish reason.
- Whether retrieval was used.
- Errors encountered during execution.

Both RAG and non-RAG requests are stored in the same table, with a `used_rag` flag distinguishing the two approaches.

## Security and Governance

Unity Catalog is used to govern access to the underlying data and AI Search index.

Access is controlled through catalog, schema, table, and index permissions.

The security approach includes:
- Restricting access to source documents and chunks.
- Controlling access to the AI Search index.
- Separating access to query logs from ordinary application users.
- Applying least-privilege permissions to application identities.
