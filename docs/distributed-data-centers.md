# Mô phỏng SFC trên nhiều data center

Chạy mã nguồn trực tiếp từ repository bằng `main.py --config <file.yaml>`. Mô-đun `virne.distributed` bổ sung bài toán DC phân cấp vào repository Virne; nó có vòng lặp sự kiện và solver riêng vì mô hình VNE cũ không phân biệt router, switch và server.

AHQ-SFC và bộ cấu hình hybrid mới được mô tả trong [hướng dẫn AHQ-SFC](ahq-sfc-running.md). Các baseline ở tài liệu này tiếp tục hoạt động với controller delay mặc định tắt.

## Cài thư viện và chạy

Python 3.12 trở lên. Trong thư mục repository, trên lệnh:

```bash
python -m pip install -r requirements-simulation.txt
python main.py --config configs/baselines/distributed_hre.yaml
```

Nếu Python đang dùng đã có NetworkX và PyYAML, chỉ cần:

```bash
python main.py --config configs/baselines/distributed_hre.yaml
python main.py --config configs/baselines/distributed_hre.yaml --validate-only
python main.py --help
```

Không cần cài package `virne`, PyTorch, PyG, Hydra hoặc OR-Tools cho các baseline DC này. Không thay đổi thư mục làm việc khi chạy. Đường dẫn YAML được tính từ thư mục gọi lệnh; `experiment.output_dir` cũng tính từ thư mục đó. Có thể dùng đường dẫn tuyệt đối, kể cả đường dẫn có dấu cách.

## Các baseline có sẵn

Mỗi file trong `configs/baselines/` là một cấu hình độc lập, có đủ tham số và không kế thừa file khác. Khi tự tạo YAML rút gọn, các khóa thiếu lấy mặc định từ `virne/distributed/config.py`; khóa không hợp lệ bị từ chối để phát hiện lỗi gõ.

| File YAML | Placement | Phạm vi |
| --- | --- | --- |
| `centralized_ffd.yaml` | First-Fit Decreasing | 1 DC, 64 server |
| `centralized_hre.yaml` | HRE-inspired | 1 DC, 64 server |
| `distributed_random.yaml` | Random với seed cố định | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_ffd.yaml` | First-Fit Decreasing | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_best_fit.yaml` | Best-Fit | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_hre.yaml` | HRE-inspired | 4 DC × 16 server, mỗi SFC trong một DC |
| `distributed_cross_dc.yaml` | HRE-inspired, thêm tập server toàn mạng | Cho phép các VNF của một SFC ở nhiều DC |
| `distributed_hre_fat_tree.yaml` | HRE-inspired | 4 DC, mỗi DC là fat-tree k=4, 16 server |

Tất cả cấu hình có cùng 64 server, tổng 512 CPU core và 4096 GB RAM, cùng WAN, seed và workload. Dùng `centralized_hre` với `distributed_hre` để so sánh vị trí DC; dùng bốn cấu hình distributed Random/FFD/Best-Fit/HRE để so sánh thuật toán. Cấu hình fat-tree thay đổi fabric nên cần phân tích riêng ảnh hưởng của topology.

## Mô hình vật lý và workload

```text
WAN router / ingress
       |
   DC gateway
       |
  spine / core
       |
  leaf / aggregation + edge
       |
  physical servers (CPU, RAM)
```

- `topology.wan`: đồ thị router vô hướng với danh sách cạnh do người dùng chỉ định; mặc định là WAN tổng hợp gồm 4 điểm biên và 1 điểm trung tâm. Đây không phải topology Abilene của paper. `bandwidth_mbps: null` mô phỏng WAN không giới hạn băng thông; các đường nội bộ DC vẫn có giới hạn riêng.
- `topology.data_centers`: mỗi DC có ID, router WAN nối vào và số server. Có thể ghi đè `cpu` và `memory_gb` ngay trong từng DC để mô phỏng năng lực không đồng nhất. Gateway, switch và router không được dùng để đặt VNF.
- `topology.fabric.type: leaf_spine`: mỗi server nối vào một leaf; các leaf nối tới mọi spine; các spine nối gateway. Cho phép số server bất kỳ.
- `topology.fabric.type: fat_tree`: fat-tree đầy đủ k-ary, k chẵn, k pod, mỗi pod có k/2 edge và k/2 aggregation switch, tổng k²/4 core switch và k³/4 server **trong mỗi DC**. Gateway nối các core. Mọi DC dùng cùng k.
- Link access từ WAN router tới gateway dùng bandwidth và latency của fabric. Mọi link vô hướng có một dung lượng dùng chung cho cả hai chiều; không mô phỏng full-duplex thành hai dung lượng độc lập. WAN và fabric có độ trễ riêng.
- Arrival là Poisson, tức khoảng cách giữa các arrival có phân phối exponential; lifetime cũng exponential. Đơn vị cấu hình là request/giờ và giờ; event dùng giây. Yêu cầu được xử lý online, departure trước arrival nếu cùng thời điểm.
- Workload là **chuỗi tuyến tính có thứ tự**: ingress → VNF0 → VNF1 → ... với demand CPU/RAM cho từng VNF, bandwidth cho từng đoạn và SLO độ trễ toàn chuỗi. Có thể bật `return_to_ingress` để dự trữ cả đoạn cuối quay về ingress. Các demand lấy uniform trong khoảng `[min, max]`.
- `max_latency_ms: null` tắt SLO. Độ trễ bằng tổng độ trễ link trên các đoạn cộng processing delay cố định của từng VNF. Chưa có hàng đợi hay packet-level simulation; đây là mô hình admission/resource allocation.
- `solver.allow_colocation: false` yêu cầu các VNF **trong cùng một SFC** dùng server khác nhau. Các SFC khác nhau vẫn chia sẻ server nếu đủ tài nguyên. Bật `true` cho phép nhiều VNF cùng server, đoạn liên kết tại chỗ dùng 0 hop.

Đổi quy mô bằng cách sửa `data_centers` và `num_servers` trong YAML. Muốn so sánh công bằng, giữ tổng CPU/RAM như nhau giữa các kiến trúc và giữ workload/seed.

## Mô tả các thuật toán

**Random:** xáo trộn thứ tự DC, server và thứ tự thử đặt VNF bằng RNG riêng, sau đó first-fit vào server đủ CPU/RAM. Không làm thay đổi RNG sinh workload.

**First-Fit Decreasing:** xếp VNF giảm dần theo block size `CPU × tổng bandwidth các đoạn kề VNF` (gồm đoạn ingress và return nếu có). Thử DC gần ingress theo độ trễ đường đi, rồi server theo thứ tự cố định.

**Best-Fit:** cùng thứ tự VNF và DC với FFD, nhưng chọn server có tổng CPU và RAM còn dư chuẩn hóa nhỏ nhất sau placement. Không có bước tối ưu năng lượng.

**HRE-inspired:** thử thứ tự FFD rồi n−1 thứ tự dạng pyramid: chuyển VNF có block size lớn nhất qua từng vị trí, phần bên trái tăng dần và bên phải giảm dần. Trong mỗi thứ tự, thử nhóm server cùng rack, cùng pod, toàn DC; leaf–spine không có pod riêng nên nhóm middle trùng toàn DC và được loại trùng. Server đang hoạt động được ưu tiên. Các placement ứng viên được xếp theo số server mới cần bật, giữ ưu tiên gần ingress khi bằng nhau. Với `cross_dc`, thêm một nhóm chứa tất cả server; nhóm này cũng tham gia xếp hạng năng lượng nên có thể được chọn dù vẫn còn một placement trong một DC.

Mọi baseline route các đoạn có bandwidth lớn trước trên đồ thị đã lọc theo dung lượng còn lại, bao gồm lượng dự trữ của các đoạn trước trong cùng yêu cầu. Random/FFD/Best-Fit dùng đường có độ trễ nhỏ nhất; HRE xét tối đa `k_paths` đường ngắn theo số hop rồi chọn ít công suất switch tăng thêm nhất khi bằng số hop. Server không được dùng làm node trung chuyển. Thứ tự **thử đặt** VNF không thay đổi thứ tự xử lý hoặc các cạnh của chuỗi gốc.

Placement và routing đều là tentative đến khi toàn bộ CPU, RAM, bandwidth và SLO hợp lệ. Khi thất bại, tài nguyên đang chạy không đổi; khi SFC hết hạn, mọi resource đã đặt được giải phóng. Các heuristic không tìm tất cả mapping/path: rejection chỉ có nghĩa thuật toán không tìm được nghiệm trong tập ứng viên.

## Công suất, năng lượng và kết quả

Server có VNF: `P = idle + (peak - idle) × CPU_utilization`; server rỗng dùng `server_off_w`. Switch có link đang sử dụng dùng `switch_idle_w + số port đang hoạt động × switch_port_w`; switch rỗng dùng `switch_off_w`. WAN router luôn bật nếu `wan_always_on: true`. Đếm port ở mỗi đầu switch của link; NIC của server nằm trong công suất server. Mô hình không có thời gian khởi động, PUE/cooling, DVFS hoặc port speed thích nghi.

Tích phân công suất theo khoảng thời gian **giữa các sự kiện**, đổi W·s sang kWh bằng chia 3.600.000. Cửa sổ đo của mọi baseline là `[0, max(arrival + lifetime của tất cả request được sinh)]`, kể cả request bị từ chối. Nhờ đó baseline từ chối nhiều request không tự có cửa sổ đo ngắn hơn. Các mean utilization cũng được tính trọng số thời gian trên cửa sổ này.

Mỗi lần chạy tạo thư mục riêng:

```text
results/distributed/<experiment.name>/<UTC-time>-<unique-id>/
  config.yaml       # cấu hình sau khi điền mặc định
  manifest.json     # file nguồn, checksum mã nguồn/workload, phiên bản Python/thư viện
  topology.json     # node/edge và tài nguyên ban đầu
  workload.json     # toàn bộ yêu cầu được sinh, kể cả yêu cầu bị từ chối
  allocations.json  # ánh xạ VNF và đường đi cho yêu cầu được nhận
  records.csv       # mỗi request: kết quả, latency, DC, thời gian solver
  events.csv        # mỗi sự kiện: active request/server/switch, utilization, power
  per_dc.csv        # số VNF/request phục vụ và utilization từng DC
  summary.json
  summary.csv
```

`summary` gồm acceptance ratio, rejection theo compute/bandwidth/latency, energy tổng/server/network, mean power, utilization, active server, latency trung bình/p95, cross-DC request và thời gian solver. Lý do rejection ghi ràng buộc sâu nhất đã thử: latency nếu từng route đủ bandwidth nhưng vi phạm SLO; bandwidth nếu từng placement đủ CPU/RAM nhưng route thất bại; còn lại compute.

`energy_kwh_per_accepted_request` là tổng năng lượng toàn hệ thống/các request được nhận, gồm cả năng lượng nền. Không gọi đại lượng này là công suất mỗi SFC. Khi không có request được nhận, latency/energy-per-request là null. Với WAN không giới hạn, utilization WAN là null vì không có mẫu số hữu hạn.

Kiểm tra `workload_sha256` giống nhau trước khi đối chiếu baseline. Cùng seed, workload và cấu hình cho cùng placement/metrics; thời gian thực thi và tên thư mục kết quả không cố định. Để kết luận nghiên cứu, cần nhiều seed và mức tải, không suy rộng từ một lần chạy demo.

## Kiểm thử

```bash
python -m pip install -r requirements-hybrid.txt
python -m unittest discover -s tests/distributed -v
```

Bộ kiểm thử chung có thêm NumPy cho QAOA/policy offline. Các test kiểm tra topology, công bằng workload, CPU/RAM, chia sẻ băng thông, rollback, release, SLO, cross-DC, nguyên vẹn thứ tự chain, tích phân năng lượng, departure cùng thời điểm, seed và CLI từ ngoài repository. Bộ kiểm thử Virne cũ trong `tests/network`, `tests/core`, `tests/solver` vẫn cần dependency của framework cũ; `requirements-simulation.txt` chỉ phục vụ mô hình DC mới.
