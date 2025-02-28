#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <liburing.h>

#define BUFFER_SIZE 4096
#define QDRANT_HOST "127.0.0.1"
#define QDRANT_PORT 6333

// Function to send HTTP requests using io_uring
int send_http_request(const char *request) {
    int sockfd;
    struct sockaddr_in server_addr;
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe;
    char buffer[BUFFER_SIZE];
    int bytes_received, response_code = 0;

    // Initialize io_uring
    io_uring_queue_init(8, &ring, 0);

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Socket error");
        return -1;
    }

    // Configure server address
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(QDRANT_PORT);
    inet_pton(AF_INET, QDRANT_HOST, &server_addr.sin_addr);

    // Connect to Qdrant
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connect error");
        close(sockfd);
        return -1;
    }
    printf("Connected to Qdrant successfully!\n");

    // Send request using io_uring
    sqe = io_uring_get_sqe(&ring);
    io_uring_prep_send(sqe, sockfd, request, strlen(request), 0);
    io_uring_submit(&ring);
    io_uring_wait_cqe(&ring, &cqe);
    
    if (cqe->res < 0) {
        printf("io_uring request failed: %d\n", cqe->res);
        io_uring_cqe_seen(&ring, cqe);
        close(sockfd);
        io_uring_queue_exit(&ring);
        return -1;
    }
    io_uring_cqe_seen(&ring, cqe);
    printf("Request sent! Waiting for response...\n");

    // Setup select() for timeout handling
    fd_set set;
    struct timeval timeout;
    FD_ZERO(&set);
    FD_SET(sockfd, &set);
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;

    int rv = select(sockfd + 1, &set, NULL, NULL, &timeout);
    if (rv == -1) {
        perror("Select error");
        close(sockfd);
        io_uring_queue_exit(&ring);
        return -1;
    } else if (rv == 0) {
        printf("Recv timeout: No response from Qdrant.\n");
        close(sockfd);
        io_uring_queue_exit(&ring);
        return -1;
    }

    // Receive response
    memset(buffer, 0, BUFFER_SIZE);
    bytes_received = recv(sockfd, buffer, BUFFER_SIZE, 0);
    if (bytes_received <= 0) {
        perror("Recv error or timeout");
        close(sockfd);
        io_uring_queue_exit(&ring);
        return -1;
    }

    // Extract HTTP response code
    sscanf(buffer, "HTTP/1.1 %d", &response_code);
    printf("Response Code: %d\n", response_code);
    printf("Raw Response:\n%s\n", buffer);

    close(sockfd);
    io_uring_queue_exit(&ring);
    return response_code;
}

// Function to create collection
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
    
    if (response_code == 400) { // Bad request (Qdrant returns this if collection exists)
        printf("Collection already exists. Skipping creation.\n");
    } else if (response_code != 200) {
        printf("Failed to create collection! Qdrant might not be running correctly.\n");
        exit(1);
    }
}

// Function to insert vector
void insert_vector() {
    printf("Inserting Vectors...\n");
    const char *request =
        "POST /collections/my_collection/points HTTP/1.1\r\n"
        "Host: " QDRANT_HOST "\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 150\r\n"
        "\r\n"
        "{\"points\": [{\"id\": 1, \"vector\": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]}]}";

    int response_code = send_http_request(request);
    if (response_code != 200) {
        printf("Vector insertion failed! Response code: %d\n", response_code);
    }
}

// Function to retrieve a vector
void retrieve_vector() {
    printf("Retrieving Vector...\n");
    const char *request =
        "GET /collections/my_collection/points/1 HTTP/1.1\r\n"
        "Host: " QDRANT_HOST "\r\n"
        "Content-Type: application/json\r\n"
        "\r\n";

    int response_code = send_http_request(request);
    if (response_code == 404) {
        printf("Vector not found!\n");
    }
}

// Function to delete a vector
void delete_vector() {
    printf("Deleting Vector...\n");
    const char *request =
        "DELETE /collections/my_collection/points/1 HTTP/1.1\r\n"
        "Host: " QDRANT_HOST "\r\n"
        "Content-Type: application/json\r\n"
        "\r\n";

    int response_code = send_http_request(request);
    if (response_code == 404) {
        printf("Vector already deleted or not found.\n");
    }
}

// Main function
int main() {
    create_collection();
    insert_vector();
    retrieve_vector();
    delete_vector();
    return 0;
}