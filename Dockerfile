FROM gcc:latest
WORKDIR /home/deysa/redis
COPY . .
# Compile only the necessary source files containing the correct main() (avoid files like server.cpp and client.cpp)
RUN g++ -O3 -march=native -flto -Wall -Wextra avl.cpp heap.cpp zset.cpp hashtable.cpp thread_pool.cpp server.cpp -o server
EXPOSE 7171
CMD ["./server"]
