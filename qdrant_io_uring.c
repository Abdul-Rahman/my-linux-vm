#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <liburing.h>
#include <fcntl.h>

#define QDRANT_HOST "127.0.0.1"
#define QDRANT_PORT 6333
#define BUFFER_SIZE 4096

struct io_uring ring;

// Function to send HTTP requests using io_uring
int send_http_request(const char *request) {
    struct sockaddr_in server_addr;
    int sockfd, ret;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    struct __kernel_timespec timeout = { .tv_sec = 5, .tv_nsec = 0 };
    char buffer[BUFFER_SIZE];

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Socket error");
        return -1;
    }

    // Setup server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(QDRANT_PORT);
    inet_pton(AF_INET, QDRANT_HOST, &server_addr.sin_addr);

    // Connect to Qdrant
    printf("Connecting to Qdrant at %s:%d...\n", QDRANT_HOST, QDRANT_PORT);
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connect error");
        close(sockfd);
        return -1;
    }
    printf("Connected to Qdrant successfully!\n");

    // Manually send HTTP request to ensure it's not stuck
    printf("Manually sending request...\n");
    send(sockfd, request, strlen(request), 0);
    printf("Request sent!\n");

    // Set timeout for receiving response
    struct timeval tv;
    tv.tv_sec = 5;
    tv.tv_usec = 0;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof(tv));

    // Read response
    memset(buffer, 0, BUFFER_SIZE);
    int bytes_received = recv(sockfd, buffer, BUFFER_SIZE, 0);
    if (bytes_received <= 0) {
        perror("Recv error or timeout");
        close(sockfd);
        return -1;
    }
    
    printf("Response: %s\n", buffer);
    close(sockfd);
    return 0;
}

void create_collection() {
    printf("Creating Collection...\n");
    const char *request =
        "PUT /collections/my_collection HTTP/1.1\r\n"
        "Host: " QDRANT_HOST "\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 85\r\n"
        "\r\n"
        "{\"name\": \"my_collection\", \"vectors\": {\"size\": 128, \"distance\": \"Cosine\"}}";

    int response_code = send_http_request(request);
    
    // If collection already exists, continue execution
    if (response_code == 400) { // Bad request (Qdrant returns this if collection exists)
        printf("Collection already exists. Skipping creation.\n");
    } else if (response_code != 200) {
        printf("Failed to create collection! Qdrant might not be running correctly.\n");
        exit(1);
    }
}

// Insert Vector
void insert_vector() {
    printf("Inserting Vectors...\n");
    const char *request =
        "POST /collections/my_collection/points HTTP/1.1\r\n"
        "Host: " QDRANT_HOST "\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 200\r\n"
        "\r\n"
        "{\"points\": [{\"id\": 1, \"vector\": [0.1, 0.2, 0.3]}]}";
    send_http_request(request);
}

// Retrieve Vector
void retrieve_vector() {
    printf("Retrieving Vector...\n");
    const char *request =
        "GET /collections/my_collection/points/1 HTTP/1.1\r\n"
        "Host: " QDRANT_HOST "\r\n"
        "\r\n";
    send_http_request(request);
}

// Delete Vector
void delete_vector() {
    printf("Deleting Vector...\n");
    const char *request =
        "DELETE /collections/my_collection/points/1 HTTP/1.1\r\n"
        "Host: " QDRANT_HOST "\r\n"
        "\r\n";
    send_http_request(request);
}

int main() {
    // Initialize io_uring
    io_uring_queue_init(32, &ring, 0);

    // Perform operations
    create_collection();
    insert_vector();
    retrieve_vector();
    delete_vector();

    // Cleanup
    io_uring_queue_exit(&ring);
    return 0;
}
