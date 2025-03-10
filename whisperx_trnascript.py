
import os
import time
import torch
import whisperx
import boto3
import math

s3_client = boto3.client("s3")

def merge_missing_timestamps(word_segments):
    merged_segments = []
    for seg in word_segments:
        if 'start' not in seg or 'end' not in seg:
            if merged_segments:
                prev_word = merged_segments[-1].get('word', '')
                curr_word = seg.get('word', '')
                merged_segments[-1]['word'] = (prev_word + " " + curr_word).strip()
            else:
                merged_segments.append(seg)
        else:
            merged_segments.append(seg)
    return merged_segments

def chunk_word_segments(word_segments, chunk_size):
    lines = []
    current_chunk_index = 0
    current_chunk_end = chunk_size

    lines.append(f"Chunk {current_chunk_index} (0 - {current_chunk_end} seconds):")
    for seg in word_segments:
        start = seg["start"]
        end = seg["end"]
        text = seg.get("text") or seg.get("word") or "<NO_TEXT_FIELD>"

        # Move to the correct chunk if needed
        while start >= current_chunk_end:
            current_chunk_index += 1
            new_chunk_start = current_chunk_end
            current_chunk_end += chunk_size
            lines.append("")
            lines.append(f"Chunk {current_chunk_index} ({new_chunk_start} - {current_chunk_end} seconds):")
        lines.append(f"{start:.2f} --> {end:.2f}: {text}")
    return "\n".join(lines)

def list_all_s3_objects(bucket, prefix=None):
    """
    Returns a list of all objects under 'prefix' in 'bucket' using S3 pagination.
    """
    continuation_token = None
    all_objects = []
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )
        contents = response.get("Contents", [])
        all_objects.extend(contents)

        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break
    return all_objects

def list_all_s3_objects_noprefix(bucket):
    """
    Returns a list of all objects under 'prefix' in 'bucket' using S3 pagination.
    """
    continuation_token = None
    all_objects = []
    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
            )
        contents = response.get("Contents", [])
        all_objects.extend(contents)

        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break
    return all_objects

def process_single_file(
    model,
    source_bucket,
    source_key,
    target_bucket,
    target_prefix,
    local_audio_dir,
    device="cuda"
):
    os.makedirs(local_audio_dir, exist_ok=True)

    audio_filename = os.path.basename(source_key)
    base_name, _ = os.path.splitext(audio_filename)
    local_audio_path = os.path.join(local_audio_dir, audio_filename)

    print(f"\nDownloading s3://{source_bucket}/{source_key} to {local_audio_path} ...")
    s3_client.download_file(source_bucket, source_key, local_audio_path)

    print(f"Transcribing {local_audio_path} ...")
    total_start_time = time.time()

    result = model.transcribe(local_audio_path)
    detected_language = result["language"]

    model_a, metadata = whisperx.load_align_model(
        language_code=detected_language,
        device=device
    )

    aligned_result = whisperx.align(
        result["segments"],
        model_a,
        metadata,
        local_audio_path,
        device=device,
        return_char_alignments=False
    )

    total_end_time = time.time()
    print(f"Total transcription + alignment time: {total_end_time - total_start_time:.2f} s")

    transcript_text = " ".join(segment["text"] for segment in aligned_result["segments"])
    transcript_filename = f"{base_name}_transcript.txt"
    transcript_path = os.path.join(local_audio_dir, transcript_filename)

    word_timestamps_filename = f"{base_name}_word_timestamps.txt"
    word_timestamps_path = os.path.join(local_audio_dir, word_timestamps_filename)

    chunked_30s_filename = f"{base_name}_30sec_timestamps.txt"
    chunked_30s_path = os.path.join(local_audio_dir, chunked_30s_filename)

    chunked_60s_filename = f"{base_name}_60sec_timestamps.txt"
    chunked_60s_path = os.path.join(local_audio_dir, chunked_60s_filename)

    word_segments = aligned_result.get("word_segments", [])
    word_segments = merge_missing_timestamps(word_segments)

    # Save full transcript
    with open(transcript_path, "w", encoding="utf-8") as f:
        f.write(transcript_text)

    # Save all word timestamps
    with open(word_timestamps_path, "w", encoding="utf-8") as f:
        for wseg in word_segments:
            wstart = wseg["start"]
            wend = wseg["end"]
            wtext = wseg.get("text") or wseg.get("word") or "<NO_TEXT_FIELD>"
            line = f"{wstart:.2f} --> {wend:.2f}: {wtext}"
            f.write(line + "\n")

    # Save chunked by 30s
    chunked_30s_text = chunk_word_segments(word_segments, chunk_size=30)
    with open(chunked_30s_path, "w", encoding="utf-8") as f:
        f.write(chunked_30s_text)

    # Save chunked by 60s
    chunked_60s_text = chunk_word_segments(word_segments, chunk_size=60)
    with open(chunked_60s_path, "w", encoding="utf-8") as f:
        f.write(chunked_60s_text)

    # Upload files back to S3
    for local_txt_path in [
        transcript_path,
        word_timestamps_path,
        chunked_30s_path,
        chunked_60s_path
    ]:
        fname = os.path.basename(local_txt_path)
        s3_key = os.path.join(target_prefix, "transcripts", fname)
        print(f"Uploading {local_txt_path} to s3://{target_bucket}/{s3_key} ...")
        s3_client.upload_file(local_txt_path, target_bucket, s3_key)

    # Clean up local files
    try:
        os.remove(local_audio_path)
    except OSError:
        pass
    for fpath in [
        transcript_path,
        word_timestamps_path,
        chunked_30s_path,
        chunked_60s_path
    ]:
        try:
            os.remove(fpath)
        except OSError:
            pass

def main():
    # Buckets and prefixes
    source_bucket = "demodaran-all-audio"
    source_prefix = "audio"
    target_bucket = "transcript-demodaran-all"
    target_prefix = "output"

    # Local dir and file extension
    local_audio_dir = "audio"
    extension = ".m4a"

    # Load WhisperX model
    device = "cuda"
    model_name = "base.en"
    if device == "cuda" and not torch.cuda.is_available():
        print("CUDA not available, using CPU instead.")
        device = "cpu"

    print(f"Loading WhisperX model '{model_name}' on device '{device}' ...")
    model = whisperx.load_model(model_name, device=device)

    # Gather all existing transcript files from target bucket (pagination handled)
    existing_base_names = set()
    transcripts_subfolder = os.path.join(target_prefix, "transcripts")

    all_transcript_objs = list_all_s3_objects(bucket=target_bucket, prefix=transcripts_subfolder)
    for item in all_transcript_objs:
        key = item["Key"]
        if key.endswith("_transcript.txt"):
            filename = os.path.basename(key)
            if filename.endswith("_transcript.txt"):
                base_name_done = filename.replace("_transcript.txt", "")
                existing_base_names.add(base_name_done)

    # Gather all audio files from source bucket (pagination handled)
    all_source_objs = list_all_s3_objects_noprefix(bucket=source_bucket)

    # Process each file unless it's already transcribed
    for item in all_source_objs:
        key = item["Key"]
        if key.endswith(extension):
            audio_filename = os.path.basename(key)
            base_name, _ = os.path.splitext(audio_filename)
            
            if base_name in existing_base_names:
                print(f"SKIPPING {key} because {base_name}_transcript.txt already exists.")
                continue
            
            print(f"Processing {key} ...")
            process_single_file(
                model=model,
                source_bucket=source_bucket,
                source_key=key,
                target_bucket=target_bucket,
                target_prefix=target_prefix,
                local_audio_dir=local_audio_dir,
                device=device
            )

if __name__ == "__main__":
    main()