#!/usr/bin/env python3
"""
Document to CSV Converter with Docling-based Chunking

This script converts any document to CSV format with chunked text,
using Docling's native chunking capabilities.

Usage:
    python pdf_to_markdown_simple.py <input_file_or_url> [output.csv] [--chunk-size TOKENS] [--mode words|sentences]

If output file is not specified, prints CSV to stdout.
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from docling.datamodel.accelerator_options import AcceleratorDevice, AcceleratorOptions
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.types.doc.labels import DocItemLabel


def clean_text(text: str) -> str:
    """
    Clean text by removing line numbers and extra whitespace.

    Args:
        text: Raw text from document

    Returns:
        Cleaned text
    """
    # Remove line numbers (sequences of 3 digits at start of lines or standalone)
    text = re.sub(r"^\d{3}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s+\d{3}\s+", " ", text)
    text = re.sub(r"\b\d{3}\b\s*", "", text)

    # Clean up multiple spaces and newlines
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    return text


def is_ui_element(text: str) -> bool:
    """
    Check if text is likely a UI element rather than meaningful content.

    Args:
        text: Text to check

    Returns:
        True if text appears to be a UI element (social media buttons, etc.)
    """
    ui_patterns = [
        r"^share on ",
        r"^tweet",
        r"^post to ",
        r"^email",
        r"^print",
        r"^download",
        r"^subscribe",
        r"^follow us",
        r"^like us",
        r"^connect with",
        r"^sign up",
        r"^log in",
        r"^click here",
    ]
    text_lower = text.lower().strip()
    return any(re.match(pattern, text_lower) for pattern in ui_patterns)


def split_into_sentences(text: str) -> list[str]:
    """
    Split text into sentences using regex.

    Args:
        text: Text to split

    Returns:
        List of sentences
    """
    # Split by sentence boundaries (., !, ?)
    sentence_pattern = r"(?<=[.!?])\s+(?=[A-Z]|$)"
    sentences = [s.strip() for s in re.split(sentence_pattern, text) if s.strip()]

    # Filter out very short fragments (likely artifacts)
    sentences = [s for s in sentences if len(s.split()) >= 3]

    return sentences


def chunk_by_paragraphs_with_context(
    document, max_words: int | None = None
) -> list[str]:
    """
    Chunk document by paragraphs, including context (title, section headers).
    If max_words is specified, splits long paragraphs into smaller chunks.

    Each chunk contains:
    - Document title (## format)
    - Section header (## format)
    - Paragraph text (clean, no line numbers)

    Args:
        document: Docling Document object
        max_words: Maximum words per chunk. If None, keeps full paragraphs.
                   If specified, splits long paragraphs by sentences.

    Returns:
        List of markdown-formatted chunks
    """
    chunks = []

    # Get document title
    doc_title = None
    for item, level in document.iterate_items():
        if item.label == DocItemLabel.TITLE:
            doc_title = clean_text(item.text)
            break

    # Track current section (only the immediate parent section)
    current_section = None

    # Iterate through document items
    for item, level in document.iterate_items():
        # Update current section
        if item.label == DocItemLabel.SECTION_HEADER:
            current_section = clean_text(item.text)

        # Process paragraphs
        elif item.label == DocItemLabel.PARAGRAPH:
            paragraph_text = clean_text(item.text)
            if not paragraph_text:
                continue

            # Build context header
            context_header = []
            if doc_title:
                context_header.append(f"## {doc_title}\n")
            if current_section:
                context_header.append(f"## {current_section}\n")
            context_str = "".join(context_header)

            # If no max_words or paragraph is short enough, add as-is
            if max_words is None or len(paragraph_text.split()) <= max_words:
                chunks.append(f"{context_str}\n{paragraph_text}")
            else:
                # Split long paragraph into sentence-based chunks
                sentences = split_into_sentences(paragraph_text)
                current_chunk_sentences = []
                current_word_count = 0

                for sentence in sentences:
                    sentence_words = len(sentence.split())

                    # If adding this sentence exceeds limit, save current chunk
                    if (
                        current_word_count + sentence_words > max_words
                        and current_chunk_sentences
                    ):
                        chunk_text = " ".join(current_chunk_sentences)
                        chunks.append(f"{context_str}\n{chunk_text}")
                        current_chunk_sentences = [sentence]
                        current_word_count = sentence_words
                    else:
                        current_chunk_sentences.append(sentence)
                        current_word_count += sentence_words

                # Add remaining sentences
                if current_chunk_sentences:
                    chunk_text = " ".join(current_chunk_sentences)
                    chunks.append(f"{context_str}\n{chunk_text}")

        # Process tables - export to markdown
        elif item.label == DocItemLabel.TABLE:
            # TableItem has export_to_markdown() method, not .text
            try:
                table_md = item.export_to_markdown()
                if table_md:
                    table_text = clean_text(table_md)
                    if table_text:
                        context_header = []
                        if doc_title:
                            context_header.append(f"## {doc_title}\n")
                        if current_section:
                            context_header.append(f"## {current_section}\n")
                        context_str = "".join(context_header)
                        chunks.append(f"{context_str}\n**Table:**\n\n{table_text}")
            except Exception as e:
                print(f"Warning: Could not process table: {e}", file=sys.stderr)

        # Process captions
        elif item.label == DocItemLabel.CAPTION:
            # Captions have .text attribute
            if hasattr(item, "text") and item.text:
                caption_text = clean_text(item.text)
                # Filter out UI elements and very short captions
                if (
                    caption_text
                    and len(caption_text.split()) >= 5
                    and not is_ui_element(caption_text)
                ):
                    context_header = []
                    if doc_title:
                        context_header.append(f"## {doc_title}\n")
                    if current_section:
                        context_header.append(f"## {current_section}\n")
                    context_str = "".join(context_header)
                    chunks.append(f"{context_str}\n*Caption:* {caption_text}")

    return chunks


def chunk_by_sentences(text: str, sentences_per_chunk: int = 1) -> list[str]:
    """
    Chunk text by sentences.

    Args:
        text: Text to chunk
        sentences_per_chunk: Number of sentences per chunk (default: 1)

    Returns:
        List of text chunks, each containing the specified number of sentences
    """
    import re

    # Remove all line numbers (sequences of 3 digits with optional spaces)
    # This handles patterns like "001 002 003" or "001" at line starts
    text = re.sub(r"\b\d{3}\b\s*", "", text)

    # Clean up multiple spaces
    text = re.sub(r"\s+", " ", text)

    # Split by sentence boundaries (., !, ?)
    # This regex looks for sentence-ending punctuation followed by whitespace and a capital letter or end of string
    sentence_pattern = r"(?<=[.!?])\s+(?=[A-Z]|$)"
    sentences = [s.strip() for s in re.split(sentence_pattern, text) if s.strip()]

    # Filter out very short fragments (likely artifacts) and markdown headers
    sentences = [s for s in sentences if len(s.split()) >= 3 and not s.startswith("#")]

    chunks = []
    for i in range(0, len(sentences), sentences_per_chunk):
        chunk = " ".join(sentences[i : i + sentences_per_chunk])
        if chunk:
            chunks.append(chunk)

    return chunks


def simple_chunk_text(text: str, max_tokens: int = 1024) -> list[str]:
    """
    Simple text chunking by paragraphs and sentences.

    Args:
        text: Text to chunk
        max_tokens: Approximate max tokens per chunk (using word count as proxy)

    Returns:
        List of text chunks
    """
    # Split by double newlines (paragraphs)
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]

    chunks = []
    current_chunk = []
    current_words = 0
    max_words = max_tokens  # Rough approximation: 1 token ≈ 1 word

    for para in paragraphs:
        para_words = len(para.split())

        # If single paragraph is too large, split by sentences
        if para_words > max_words:
            sentences = [s.strip() + "." for s in para.split(".") if s.strip()]
            for sent in sentences:
                sent_words = len(sent.split())
                if current_words + sent_words > max_words and current_chunk:
                    chunks.append(" ".join(current_chunk))
                    current_chunk = [sent]
                    current_words = sent_words
                else:
                    current_chunk.append(sent)
                    current_words += sent_words
        else:
            # Check if adding this paragraph exceeds limit
            if current_words + para_words > max_words and current_chunk:
                chunks.append(" ".join(current_chunk))
                current_chunk = [para]
                current_words = para_words
            else:
                current_chunk.append(para)
                current_words += para_words

    # Add remaining chunk
    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks


def handle_text_file(
    file_path: str, chunk_size: int = 1024, chunking_mode: str = "words"
) -> pd.DataFrame:
    """
    Handle plain text files (.txt) by reading and chunking them directly.
    Docling does not support .txt files, so we handle them separately.

    Args:
        file_path: Path to the text file
        chunk_size: Max words per chunk
        chunking_mode: Chunking strategy

    Returns:
        DataFrame with single 'text' column containing chunks
    """
    print(f"[1/2] Reading text file: {file_path}", file=sys.stderr)
    print(
        f"    (Plain .txt files are not supported by Docling, handling directly)",
        file=sys.stderr,
    )

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    print(f"[2/2] Chunking text (mode: {chunking_mode})...", file=sys.stderr)

    # Chunk the text
    if chunking_mode == "sentences":
        chunks = chunk_by_sentences(text, sentences_per_chunk=chunk_size)
        print(
            f"\n✓ Created {len(chunks)} chunks ({chunk_size} sentence(s) per chunk)",
            file=sys.stderr,
        )
    else:  # words mode or any other
        chunks = simple_chunk_text(text, max_tokens=chunk_size)
        print(
            f"\n✓ Created {len(chunks)} chunks (~{chunk_size} words per chunk)",
            file=sys.stderr,
        )

    if chunks:
        print(
            f"  Average words per chunk: {sum(len(c.split()) for c in chunks) / len(chunks):.0f}",
            file=sys.stderr,
        )
        print(
            f"  Total words: {sum(len(c.split()) for c in chunks):,}", file=sys.stderr
        )

    return pd.DataFrame({"text": chunks})


def document_to_csv(
    input_source: str,
    output_path: str | None = None,
    chunk_size: int = 1024,
    chunking_mode: str = "paragraphs",
) -> pd.DataFrame:
    """
    Convert any document to CSV with chunked text using Docling structure.

    Docling's DocumentConverter.convert() natively handles:
    - Local file paths (PDF, DOCX, PPTX, images, HTML, MD, etc.)
    - URLs (http://, https://)
    - Multiple document formats automatically

    Note: Plain .txt files are NOT supported by Docling and are handled separately.

    Args:
        input_source: Path to local file OR URL to document
        output_path: Optional path to save CSV file
        chunk_size: Max words per chunk (used for fine_paragraphs and words modes)
        chunking_mode: Chunking strategy:
            - "paragraphs": Full paragraphs with context (may be long)
            - "fine_paragraphs": Paragraphs split by sentences with max_words limit
            - "sentences": Sentence-based chunks
            - "words": Word-based chunks

    Returns:
        DataFrame with single 'text' column containing chunks
    """
    # Check if it's a .txt file - Docling doesn't support plain text
    if input_source.lower().endswith(".txt"):
        df = handle_text_file(input_source, chunk_size, chunking_mode)
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_file, index=False)
            print(f"\n✓ Saved to: {output_file}", file=sys.stderr)
        return df

    print(f"[1/3] Initializing document converter...", file=sys.stderr)

    # Initialize converter with OCR
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = True
    pipeline_options.do_table_structure = True
    pipeline_options.ocr_options.lang = ["en", "fr"]
    pipeline_options.accelerator_options = AcceleratorOptions(
        num_threads=4, device=AcceleratorDevice.AUTO
    )

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options),
        }
    )

    print(f"[2/3] Converting document from: {input_source}", file=sys.stderr)
    print(f"    (Docling handles URLs and local files automatically)", file=sys.stderr)

    # Docling's convert() handles both local files and URLs natively
    result = converter.convert(input_source)
    document = result.document

    print(f"[3/3] Chunking document (mode: {chunking_mode})...", file=sys.stderr)

    # Chunk using Docling document structure
    if chunking_mode == "paragraphs":
        # Full paragraphs (may be long)
        chunks = chunk_by_paragraphs_with_context(document, max_words=None)
        print(
            f"\n✓ Created {len(chunks)} paragraph chunks with context", file=sys.stderr
        )
    elif chunking_mode == "fine_paragraphs":
        # Fine-grained paragraphs split by sentences with max_words limit
        chunks = chunk_by_paragraphs_with_context(document, max_words=chunk_size)
        print(
            f"\n✓ Created {len(chunks)} fine-grained paragraph chunks (max {chunk_size} words)",
            file=sys.stderr,
        )
    elif chunking_mode == "sentences":
        # Fallback to sentence chunking
        markdown_text = document.export_to_markdown()
        chunks = chunk_by_sentences(markdown_text, sentences_per_chunk=chunk_size)
        print(
            f"\n✓ Created {len(chunks)} chunks ({chunk_size} sentence(s) per chunk)",
            file=sys.stderr,
        )
    else:  # words mode
        markdown_text = document.export_to_markdown()
        chunks = simple_chunk_text(markdown_text, max_tokens=chunk_size)
        print(
            f"\n✓ Created {len(chunks)} chunks (~{chunk_size} words per chunk)",
            file=sys.stderr,
        )

    if chunks:
        print(
            f"  Average words per chunk: {sum(len(c.split()) for c in chunks) / len(chunks):.0f}",
            file=sys.stderr,
        )
        print(
            f"  Total words: {sum(len(c.split()) for c in chunks):,}", file=sys.stderr
        )

    # Create DataFrame
    df = pd.DataFrame({"text": chunks})

    # Save or print CSV
    if output_path:
        output_file = Path(output_path)
        df.to_csv(output_file, index=False, encoding="utf-8")
        print(f"\n✓ CSV saved to: {output_file}", file=sys.stderr)
        print(f"  Rows: {len(df)}", file=sys.stderr)
    else:
        # Print to stdout
        print(df.to_csv(index=False), end="")

    return df


def main():
    """Main entry point for CLI usage."""
    parser = argparse.ArgumentParser(
        description="Convert any document to CSV with chunked text (simple chunking)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert PDF to CSV
  python pdf_to_markdown_simple.py document.pdf output.csv

  # From URL
  python pdf_to_markdown_simple.py https://arxiv.org/pdf/2207.02720 output.csv

  # Print to stdout
  python pdf_to_markdown_simple.py document.pdf

  # Custom chunk size
  python pdf_to_markdown_simple.py document.pdf output.csv --chunk-size 2048
        """,
    )

    parser.add_argument("input", help="Path to document file or URL")
    parser.add_argument("output", nargs="?", help="Output CSV file (optional)")
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1024,
        help="Maximum tokens per chunk (default: 1024)",
    )

    args = parser.parse_args()

    try:
        document_to_csv(
            args.input,
            args.output,
            chunk_size=args.chunk_size,
        )

    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

# Made with Bob
