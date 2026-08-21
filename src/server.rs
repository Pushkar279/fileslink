use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::{Body, Bytes},
    extract::{self, DefaultBodyLimit, Query, State},
    response::{Html, Response},
    routing::{get, post},
    Json, Router,
};

use http::{header::CONTENT_TYPE, StatusCode};

use log::{debug, error, info, warn};

use teloxide::net::Download;
use teloxide::payloads::SendDocumentSetters;
use teloxide::prelude::Requester;
use teloxide::types::{ChatId, InputFile};

use uuid::Uuid;

use shared::file_storage::{
    get_file_metadata,
    list_all_files,
    save_file_metadata,
    FileMetadata,
};

use crate::config::Config;
use shared::link_utils::extract_id_from_path;


// ============================================================
// APP STATE
// ============================================================

#[derive(Clone)]
pub struct AppState {
    pub bot: Arc<teloxide::Bot>,
}


// ============================================================
// CREATE APP
// ============================================================

pub async fn create_app(bot: Arc<teloxide::Bot>) -> Router {
    let enable_files_route =
        Config::instance()
            .await
            .enable_files_route();

    let state = AppState { bot };

    let mut router = Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/files/:id", get(files_id))
        .route("/api/files", get(files_api))
        .route("/api/files/upload", post(files_upload))
        .with_state(state);

    if enable_files_route {
        router = router.route("/files", get(files_list));
    }

    // Maximum request body size.
    //
    // This protects the server from accidentally receiving
    // extremely large requests.
    //
    // Telegram Bot API limits apply separately.
    router = router.layer(
        DefaultBodyLimit::max(50 * 1024 * 1024),
    );

    router.fallback(not_found_handler)
}


// ============================================================
// HEALTH CHECK
// ============================================================

async fn health() -> &'static str {
    "OK"
}


// ============================================================
// FASTTELETHON LARGE FILE DOWNLOAD
// ============================================================

async fn proxy_to_fasttelethon(
    metadata: &FileMetadata,
    force_download: bool,
) -> Result<Response<Body>, Infallible> {

    let config = Config::instance().await;

    let fasttelethon_url =
        config.fasttelethon_url();

    let channel_id =
        match config.storage_channel_id() {

            Ok(id) => id.to_string(),

            Err(_) => {
                error!(
                    "STORAGE_CHANNEL_ID not configured"
                );

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::SERVICE_UNAVAILABLE
                        )
                        .body(
                            Body::from(
                                "Large file download service not configured"
                            )
                        )
                        .unwrap()
                );
            }
        };

    let message_id =
        match metadata.message_id {

            Some(id) => id,

            None => {
                warn!(
                    "File {} has no message_id",
                    metadata.unique_id
                );

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::NOT_FOUND
                        )
                        .body(
                            Body::from(
                                "File cannot be downloaded through the large-file service. Please re-upload it."
                            )
                        )
                        .unwrap()
                );
            }
        };

    let download_url = format!(
        "{}/download/{}/{}",
        fasttelethon_url.trim_end_matches('/'),
        channel_id,
        message_id
    );

    info!(
        "Streaming large file {} through FastTelethon",
        metadata.file_name
    );

    let client = reqwest::Client::new();

    let response =
        match client
            .get(&download_url)
            .send()
            .await
        {

            Ok(response) => response,

            Err(e) => {
                error!(
                    "Failed to connect to FastTelethon: {:?}",
                    e
                );

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::SERVICE_UNAVAILABLE
                        )
                        .body(
                            Body::from(
                                "Download service temporarily unavailable"
                            )
                        )
                        .unwrap()
                );
            }
        };

    if !response.status().is_success() {

        error!(
            "FastTelethon returned HTTP {}",
            response.status()
        );

        return Ok(
            Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(
                    Body::from(
                        "Failed to retrieve file from download service"
                    )
                )
                .unwrap()
        );
    }

    /*
     * IMPORTANT:
     *
     * Convert the header into an owned String.
     *
     * response.bytes_stream() consumes response.
     * Therefore we cannot keep a borrowed &str from response
     * alive while consuming response.
     */
    let content_type =
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_owned())
            .unwrap_or_else(|| {
                metadata
                    .mime_type
                    .clone()
                    .unwrap_or_else(|| {
                        "application/octet-stream"
                            .to_string()
                    })
            });

    let safe_filename =
        sanitize_filename(
            &metadata.file_name
        );

    let content_disposition =
        if force_download {

            format!(
                "attachment; filename=\"{}\"",
                safe_filename
            )

        } else {

            format!(
                "inline; filename=\"{}\"",
                safe_filename
            )
        };

    /*
     * Stream the file.
     *
     * DO NOT use response.bytes().await here.
     *
     * bytes() loads the entire file into RAM.
     *
     * bytes_stream() allows Axum to stream the response
     * to the client.
     */
    let stream =
        response.bytes_stream();

    let body =
        Body::from_stream(stream);

    Ok(
        Response::builder()
            .status(StatusCode::OK)
            .header(
                CONTENT_TYPE,
                content_type
            )
            .header(
                "Content-Disposition",
                content_disposition
            )
            .header(
                "X-Content-Type-Options",
                "nosniff"
            )
            .body(body)
            .unwrap()
    )
}


// ============================================================
// FILE LIST HTML
// ============================================================

async fn files_list()
    -> Result<Response<Body>, Infallible>
{
    info!("Files list accessed");

    let files =
        list_all_files().await;

    if files.is_empty() {

        return Ok(
            Response::builder()
                .status(StatusCode::OK)
                .header(
                    CONTENT_TYPE,
                    "text/html; charset=utf-8"
                )
                .body(
                    Body::from(
                        "<h1>Files in storage</h1>\
                         <p>No files uploaded yet.</p>"
                    )
                )
                .unwrap()
        );
    }

    let mut html =
        String::from(
            "<h1>Files in storage</h1><ul>"
        );

    for file in files {

        let filename =
            escape_html(
                &file.file_name
            );

        let unique_id =
            escape_html(
                &file.unique_id
            );

        html.push_str(
            &format!(
                "<li>\
                    <a href=\"/files/{}\">{}</a>\
                    ({} bytes)\
                </li>",
                unique_id,
                filename,
                file.file_size
            )
        );
    }

    html.push_str("</ul>");

    Ok(
        Response::builder()
            .status(StatusCode::OK)
            .header(
                CONTENT_TYPE,
                "text/html; charset=utf-8"
            )
            .body(
                Body::from(html)
            )
            .unwrap()
    )
}


// ============================================================
// FILE API
// ============================================================

async fn files_api()
    -> Json<Vec<FileMetadata>>
{
    let files =
        list_all_files().await;

    Json(files)
}


// ============================================================
// FILE UPLOAD
// ============================================================

async fn files_upload(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Response<Body>, Infallible> {

    let raw_filename =
        params
            .get("filename")
            .cloned()
            .unwrap_or_else(|| {
                "upload.bin".to_string()
            });

    let filename =
        sanitize_filename(
            &raw_filename
        );

    if body.is_empty() {

        return Ok(
            Response::builder()
                .status(
                    StatusCode::BAD_REQUEST
                )
                .body(
                    Body::from(
                        "Empty body"
                    )
                )
                .unwrap()
        );
    }

    let storage_channel_id =
        match Config::instance()
            .await
            .storage_channel_id()
        {

            Ok(id) => id,

            Err(e) => {

                error!(
                    "STORAGE_CHANNEL_ID not configured: {}",
                    e
                );

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::INTERNAL_SERVER_ERROR
                        )
                        .body(
                            Body::from(
                                "Storage channel not configured"
                            )
                        )
                        .unwrap()
                );
            }
        };

    /*
     * Generate a unique public ID.
     */
    let unique_id =
        format!(
            "u{}",
            Uuid::new_v4()
        );

    /*
     * Temporary file.
     *
     * The file is deleted after Telegram upload.
     */
    let tmp_path =
        std::env::temp_dir()
            .join(
                format!(
                    "fileslink_upload_{}",
                    unique_id
                )
            );

    /*
     * Axum Bytes keeps the upload body in memory.
     *
     * DefaultBodyLimit above limits this to 50 MB.
     */
    if let Err(e) =
        tokio::fs::write(
            &tmp_path,
            &body
        )
        .await
    {

        error!(
            "Failed to write temporary upload: {:?}",
            e
        );

        return Ok(
            Response::builder()
                .status(
                    StatusCode::INTERNAL_SERVER_ERROR
                )
                .body(
                    Body::from(
                        "Failed to write upload"
                    )
                )
                .unwrap()
        );
    }

    /*
     * Upload the file to Telegram.
     */
    let sent =
        match state
            .bot
            .send_document(
                ChatId(
                    storage_channel_id
                ),
                InputFile::file(
                    &tmp_path
                ),
            )
            .caption(
                &unique_id
            )
            .await
        {

            Ok(message) => message,

            Err(e) => {

                error!(
                    "Failed to send document to Telegram: {:?}",
                    e
                );

                let _ =
                    tokio::fs::remove_file(
                        &tmp_path
                    )
                    .await;

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::BAD_GATEWAY
                        )
                        .body(
                            Body::from(
                                "Failed to store file in Telegram"
                            )
                        )
                        .unwrap()
                );
            }
        };

    /*
     * Delete temporary file.
     */
    let _ =
        tokio::fs::remove_file(
            &tmp_path
        )
        .await;

    /*
     * Get Telegram file ID.
     */
    let stored_file_id =
        match sent.document() {

            Some(document) =>
                document.file.id.clone(),

            None => {

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::BAD_GATEWAY
                        )
                        .body(
                            Body::from(
                                "Stored message returned no file id"
                            )
                        )
                        .unwrap()
                );
            }
        };

    let file_size =
        body.len() as u32;

    let mime_type =
        mime_guess::from_path(
            &filename
        )
        .first()
        .map(
            |mime| mime.to_string()
        );

    let uploaded_at =
        SystemTime::now()
            .duration_since(
                UNIX_EPOCH
            )
            .unwrap_or_default()
            .as_secs();

    /*
     * Save metadata.
     */
    let metadata =
        FileMetadata {
            unique_id:
                unique_id.clone(),

            telegram_file_id:
                stored_file_id,

            file_name:
                filename.clone(),

            mime_type,

            file_size,

            uploaded_at,

            message_id:
                Some(sent.id.0),
        };

    if let Err(e) =
        save_file_metadata(
            metadata
        )
        .await
    {

        error!(
            "Failed to save metadata: {}",
            e
        );

        return Ok(
            Response::builder()
                .status(
                    StatusCode::INTERNAL_SERVER_ERROR
                )
                .body(
                    Body::from(
                        "Failed to save metadata"
                    )
                )
                .unwrap()
        );
    }

    info!(
        "Uploaded file: {} ({} bytes), id={}",
        filename,
        file_size,
        unique_id
    );

    let response =
        UploadResponse {
            success: true,
            unique_id,
            file_name: filename,
            size: file_size,
        };

    /*
     * Return JSON manually as an Axum Response.
     *
     * This avoids the previous IntoResponse compilation issue.
     */
    let json =
        match serde_json::to_vec(
            &response
        ) {

            Ok(json) => json,

            Err(e) => {

                error!(
                    "Failed to serialize upload response: {:?}",
                    e
                );

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::INTERNAL_SERVER_ERROR
                        )
                        .body(
                            Body::from(
                                "Failed to create response"
                            )
                        )
                        .unwrap()
                );
            }
        };

    Ok(
        Response::builder()
            .status(StatusCode::OK)
            .header(
                CONTENT_TYPE,
                "application/json"
            )
            .header(
                "Cache-Control",
                "no-store"
            )
            .body(
                Body::from(json)
            )
            .unwrap()
    )
}


// ============================================================
// FILE DOWNLOAD
// ============================================================

async fn files_id(
    State(state): State<AppState>,
    extract::Path(id): extract::Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response<Body>, Infallible> {

    debug!(
        "Requested file with path: {}",
        id
    );

    let unique_id =
        extract_id_from_path(
            &id
        );

    debug!(
        "Extracted unique ID: {}",
        unique_id
    );

    let metadata =
        match get_file_metadata(
            unique_id
        )
        .await
        {

            Some(metadata) =>
                metadata,

            None => {

                warn!(
                    "File not found with ID: {}",
                    unique_id
                );

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::NOT_FOUND
                        )
                        .header(
                            CONTENT_TYPE,
                            "text/html; charset=utf-8"
                        )
                        .body(
                            Body::from(
                                "<h1>404 Not Found</h1>\
                                 <p>The requested file does not exist.</p>\
                                 <a href=\"/\">Go back to the homepage</a>"
                            )
                        )
                        .unwrap()
                );
            }
        };

    info!(
        "Found file: {} (Telegram ID: {})",
        metadata.file_name,
        metadata.telegram_file_id
    );

    let force_download =
        params
            .get("dl")
            .is_some();

    /*
     * Try Telegram Bot API first.
     */
    let file_info =
        match state
            .bot
            .get_file(
                &metadata.telegram_file_id
            )
            .await
        {

            Ok(info) => info,

            Err(e) => {

                let error_msg =
                    format!("{:?}", e);

                /*
                 * If Telegram Bot API refuses the file because
                 * it is too large, use FastTelethon.
                 */
                if error_msg.contains("file is too big")
                    || error_msg.contains("Bad Request")
                {
                    warn!(
                        "File too large for Bot API: {}",
                        metadata.file_name
                    );

                    return proxy_to_fasttelethon(
                        &metadata,
                        force_download
                    )
                    .await;
                }

                error!(
                    "Failed to get file info from Telegram: {:?}",
                    e
                );

                return Ok(
                    Response::builder()
                        .status(
                            StatusCode::INTERNAL_SERVER_ERROR
                        )
                        .body(
                            Body::from(
                                "Failed to retrieve file from storage"
                            )
                        )
                        .unwrap()
                );
            }
        };

    /*
     * Download normal-sized files through the Telegram Bot API.
     */
    let mut file_bytes = Vec::new();

    if let Err(e) = state
        .bot
        .download_file(
            &file_info.path,
            &mut file_bytes
        )
        .await
    {
        error!(
            "Failed to download file from Telegram: {:?}",
            e
        );

        return Ok(
            Response::builder()
                .status(
                    StatusCode::INTERNAL_SERVER_ERROR
                )
                .body(
                    Body::from(
                        "Failed to download file from storage"
                    )
                )
                .unwrap()
        );
    }

    info!(
        "Successfully downloaded {} bytes from Telegram",
        file_bytes.len()
    );

    let content_type =
        if force_download {
            "application/octet-stream".to_string()
        } else {
            metadata
                .mime_type
                .clone()
                .unwrap_or_else(|| {
                    "application/octet-stream".to_string()
                })
        };

    let safe_filename =
        sanitize_filename(
            &metadata.file_name
        );

    let content_disposition =
        if force_download {
            format!(
                "attachment; filename=\"{}\"",
                safe_filename
            )
        } else {
            format!(
                "inline; filename=\"{}\"",
                safe_filename
            )
        };

    Ok(
        Response::builder()
            .status(StatusCode::OK)
            .header(
                CONTENT_TYPE,
                content_type
            )
            .header(
                "Content-Disposition",
                content_disposition
            )
            .header(
                "X-Content-Type-Options",
                "nosniff"
            )
            .body(
                Body::from(file_bytes)
            )
            .unwrap()
    )
}


// ============================================================
// ROOT
// ============================================================

async fn root() -> Html<&'static str> {
    info!("Root path accessed");

    Html(
        "<h1>Server working</h1>\
         <div><a href=\"https://github.com/Pushkar279/fileslink\">GitHub</a></div>"
    )
}


// ============================================================
// 404
// ============================================================

async fn not_found_handler() -> Html<&'static str> {
    Html(
        "<h1>404 Not Found</h1>\
         <p>The page you are looking for does not exist.</p>\
         <a href=\"/\">Go back to the homepage</a>"
    )
}


// ============================================================
// FILENAME SANITIZER
// ============================================================

fn sanitize_filename(filename: &str) -> String {
    let sanitized: String = filename
        .chars()
        .map(|c| {
            if c.is_control()
                || matches!(c, '"' | '\\' | '/' | '<' | '>' | ':')
            {
                '_'
            } else {
                c
            }
        })
        .collect();

    if sanitized.trim().is_empty() {
        "download.bin".to_string()
    } else {
        sanitized
    }
}


// ============================================================
// HTML ESCAPING
// ============================================================

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}


// ============================================================
// UPLOAD RESPONSE
// ============================================================

#[derive(serde::Serialize)]
struct UploadResponse {
    success: bool,
    unique_id: String,
    file_name: String,
    size: u32,
}
