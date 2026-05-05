import json
import os
import shutil
import tempfile
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, StreamingResponse
from analyzer import analyze_video, analyze_video_stream

app = FastAPI(title="Running Analysis API")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

app.mount("/outputs", StaticFiles(directory=OUTPUT_DIR), name="outputs")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("static/index.html") as f:
        return f.read()


@app.post("/analyze")
async def analyze(video: UploadFile = File(...)):
    ext = os.path.splitext(video.filename)[1].lower() or ".mp4"
    if ext not in [".mp4", ".mov", ".avi"]:
        raise HTTPException(status_code=400, detail="Please upload a video file (mp4, mov, avi)")

    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        shutil.copyfileobj(video.file, tmp)
        tmp_path = tmp.name

    output_filename = "processed_" + os.path.basename(tmp_path).replace(ext, ".mp4")
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    try:
        results = analyze_video(tmp_path, output_path=output_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")
    finally:
        os.unlink(tmp_path)

    if "error" in results:
        raise HTTPException(status_code=422, detail=results["error"])

    results["video_url"] = f"/outputs/{output_filename}"
    return results


@app.post("/analyze/stream")
async def analyze_stream(video: UploadFile = File(...)):
    ext = os.path.splitext(video.filename)[1].lower() or ".mp4"
    if ext not in [".mp4", ".mov", ".avi"]:
        raise HTTPException(status_code=400, detail="Please upload a video file (mp4, mov, avi)")

    content = await video.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    output_filename = "processed_" + os.path.basename(tmp_path).replace(ext, ".mp4")
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    def event_stream():
        try:
            for event in analyze_video_stream(tmp_path, output_path=output_path):
                if event["type"] == "done":
                    event["video_url"] = f"/outputs/{output_filename}"
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

@app.post("/analyze/sample")
async def analyze_sample():
    sample_path = os.path.join(os.path.dirname(__file__), "static", "SampleVideo.mov")
    if not os.path.exists(sample_path):
        raise HTTPException(status_code=404, detail="Sample video not found")

    output_filename = "processed_sample.mp4"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    def event_stream():
        try:
            for event in analyze_video_stream(sample_path, output_path=output_path):
                if event["type"] == "done":
                    event["video_url"] = f"/outputs/{output_filename}"
                yield f"data: {json.dumps(event)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )