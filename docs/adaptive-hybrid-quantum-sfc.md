# Thuật toán: Adaptive Hierarchical Hybrid SFC Embedding

Hiện đã có implementation về AHQ-SFC, xem [hướng dẫn chạy](ahq-sfc-running.md). Adapter D-Wave đã có nhưng chưa xác thực trên QPU thật. Các mục dưới đây giữ cơ sở thiết kế như migration, cross-DC quantum, CP-SAT joint model và online bandit exploration.

Chọn orchestration thích ứng làm kiến trúc chính và dùng hierarchical decomposition để tạo subproblem QUBO có ngân sách. Quantum là một backend tùy chọn trong quá trình improvement; placement, routing và kiểm tra ràng buộc vẫn có mode chạy classical.

## Phạm vi hệ thống

- Online admission và placement cho SFC mới trên nhiều DC; mỗi SFC ở một DC.
- CPU, RAM, bandwidth và latency SLO là ràng buộc cứng khi commit.
- Giữ `allow_colocation: false` như các preset hiện tại. Các SFC khác nhau vẫn chia sẻ server nếu còn tài nguyên.
- Cố định các request đang chạy; chỉ thay đổi quyết định tentative của request mới.
- Chưa đưa migration cost vào hàm mục tiêu (simulator hiện chưa có copy state, downtime, băng thông migration, destination reservation và rollback migration).
- Chưa kết hợp cross-DC placement với quantum trong lần triển khai đầu. Có thể mở rộng sau khi đã đánh giá riêng độ mất chất lượng do chọn DC/candidate.

Mục tiêu xử lý một request: tìm nghiệm khả thi trong deadline, sau đó chọn nghiệm có chi phí thấp hơn. Mục tiêu đánh giá toàn hệ thống: acceptance, SLA, năng lượng và chi phí điều phối. Nếu chỉ tối thiểu hóa năng lượng mà không tính admission có thể khiến hệ thống chọn cách từ chối mọi request, nên không dùng mục tiêu đó một mình.

## Luồng xử lý

1. Nắm bắt trạng thái residual tài nguyên và chạy HRE/Best-Fit với ngân sách nhỏ, giữ incumbent khả thi nếu có. Chưa commit.
2. Xếp hạng DC theo proximity, tài nguyên phù hợp và khả năng tiếp nhận ingress. Thử tối đa 2 DC ứng viên ban đầu, không khóa cứng vào duy nhất DC gần nhất.
3. Chọn tập VNF cần sửa U: VNF chưa đặt được, hai đầu đoạn route thất bại hoặc VNF liên quan đến server mới cần bật. Ưu tiên một đoạn liên tiếp của chain.
4. Cố định quyết định ngoài U. Tạo candidate server cho mỗi VNF trong U bằng CPU/RAM residual, ingress/neighbor reachability, locality và incremental power. Bao gồm host của incumbent nếu có để không loại bỏ phương án đã khả thi.
5. Orchestrator chọn: dùng incumbent, exact search, simulated annealing, hoặc backend quantum được cấu hình. Quyết định dựa trên đặc trưng instance, ngân sách còn lại và lợi ích đạt được qua quan sát trên tập huấn luyện/validation.
6. Decode nhiều mẫu nếu backend trả về nhiều nghiệm; bỏ mẫu vi phạm placement. Với từng placement, dựng Allocation mới và route lại **toàn bộ request**. Không tái dùng các reservations từ một lần thử thất bại.
7. Kiểm tra CPU/RAM/BW/order/SLA, so sánh chi phí thực của các nghiệm khả thi. Commit tốt nhất; timeout hoặc mẫu không khả thi thì dùng incumbent/fallback. Không có incumbent khả thi sau hết ngân sách thì từ chối request.

Mọi solver dùng cùng candidate reducer và validator khi đánh giá đóng góp backend. Giữ cả baseline classical không giảm candidate để đo tổn thất do decomposition.

## Kích thước subproblem

Các giá trị dưới đây là cấu hình khởi đầu cho thực nghiệm, không phải giới hạn QPU:

- Tối đa 2 DC ứng viên; giải từng DC riêng.
- Mặc định |U|=4, tối đa 3 server/VNF: tối đa 12 biến placement.
- Sweep |U| trong {2, 4, 6, 8}, candidate trong {2, 3, 4}; giới hạn phụ thuộc backend.
- Thử QAOA simulator bắt đầu ở 8–16 biến, p=1–2; ghi cả depth sau transpilation, số cổng hai qubit, shots và số lần optimizer gọi circuit.
- Quantum annealing lưu biến logical, qubit physical sau embedding, chain length/breaks, reads và thời gian lập trình/chạy QPU.

## QUBO placement

Đặt x[v,s]=1 nếu VNF v thuộc U được gán vào s thuộc C_v. Viết:

\[
F(x)=\sum_i a_i x_i+\sum_{i<j}b_{ij}x_ix_j,
\]

\[
H(x)=F(x)+\lambda\sum_{v\in U}\left(\sum_{s\in C_v}x_{v,s}-1\right)^2
+\mu\sum_s\sum_{\substack{v<w\\s\in C_v\cap C_w}}x_{v,s}x_{w,s}.
\]

- F là chi phí surrogate đã chuẩn hóa, gồm incremental server power và độ trễ (network cost) ước lượng theo các cặp VNF kề nhau. Các đoạn nối VNF trong U với VNF đã cố định trở thành hệ số tuyến tính.
- Penalty thứ nhất: mỗi VNF chọn đúng một server.
- Penalty thứ hai: hai VNF trong request không cùng server.
- CPU/RAM: loại trước host không đủ tài nguyên residual và host đã dùng bởi VNF cố định cùng request. Vì không colocation, mỗi host nhận tối đa một VNF mới, nên prefilter cộng ràng buộc distinct-host đủ để bảo đảm CPU/RAM trong snapshot. Lập luận này không còn đúng nếu bật colocation; khi đó phải mô hình hóa tổng demand/activation và thêm biến phụ hoặc dùng formulation khác.
- Bandwidth toàn mạng và latency SLO chưa được biểu diễn đầy đủ trong QUBO. Đường khả thi cho từng cặp riêng lẻ không bảo đảm các luồng cùng khả thi khi tranh chấp link.
- Switch power phụ thuộc vào tập hợp các thiết bị đang hoạt động.

H không phải QUBO tương đương toàn bộ bài toán joint placement-routing. Phải lưu cả QUBO energy, feasibility thực và objective thực.

Để kiểm tra penalty trên instance nhỏ, dùng bound B=sum(|a_i|)+sum(|b_ij|); chọn lambda, mu > B nếu tồn tại assignment thỏa các placement constraints. Điều này giúp ưu tiên feasibility placement trong số ground states.

Mục tiêu thực để xếp hạng các nghiệm đã qua validator có thể dùng:

\[
J=\alpha\frac{\Delta P_{\mathrm{server+network}}}{P_{ref}}
+\beta\frac{D_{SFC}}{D_{ref}}.
\]

Chọn P_ref và D_ref cố định trong một benchmark; nếu SLA bị tắt vẫn cần D_ref. Delta P là công suất tăng thêm. Năng lượng thực vẫn đo bằng tích phân công suất của event simulator. Migration để giai đoạn sau và lúc đó mới thêm gamma.

## Backend và chính sách điều phối

**Triển khai đầu tiên:** reducer + QUBO builder + exact enumeration + simulated annealing classical + validator.

**Quantum backend ưu tiên:** quantum annealing qua adapter trực tiếp tới QPU, vì dùng cùng formulation BQM. Chỉ bật khi có backend và ngân sách được cấu hình.

**QAOA:** adapter thực nghiệm thứ hai, trước hết simulator ideal/noisy trên instance nhỏ, sau đó hardware nếu có. Đây là phép so sánh khác cơ chế backend, không phải yêu cầu để hoàn thành kiến trúc đầu tiên.

Chính sách v1 là rule-based có calibration:

| Tình trạng | Quyết định |
| --- | --- |
| Có incumbent đủ tốt, deadline còn ít | Dùng incumbent |
| Số assignment sau reduction nhỏ, exact được dự báo hoàn tất kịp | Exact |
| Cần repair, exact có thể quá ngân sách, chưa có lợi ích QPU đã đo | Classical SA/local search |
| Backend QPU sẵn sàng, lợi ích dự báo vượt chi phí và đáp ứng deadline | Quantum sampler |
| Backend lỗi, timeout, trạng thái cũ hoặc mọi mẫu không khả thi | Fallback/giữ incumbent |

Sau khi có dữ liệu, dùng cost-sensitive contextual bandit để chọn backend, kích thước neighborhood và sampling budget. Action “không gọi thêm solver” luôn tồn tại. Huấn luyện offline và tách topology/seed/load validation-test; so sánh với policy rule-based.

## Thời gian quyết định

Phân biệt data-plane latency SLA với control-plane admission deadline.

T_end_to_end = preprocessing + formulation + embedding/transpilation + queue + backend_execution + decode/repair + routing_validation.

Không chỉ báo anneal time hoặc circuit execution. Chi phí và số QPU call được lưu riêng; reward nếu cần phải chuẩn hóa trước khi cộng các đại lượng.

Đường chạy cũ mặc định solve tại arrival. Implementation mới có tùy chọn `control_plane.enabled` với worker queue và event decision_complete; xem hướng dẫn chạy để phân biệt modeled/measured/trace latency và giới hạn mô hình concurrency.

Để đánh giá orchestration: thêm event decision_complete, queue và worker capacity; quy định ngân sách controller giống nhau giữa các baseline. Trạng thái có thể đổi giữa snapshot và completion: kiểm tra lại residual/version rồi commit nguyên tử. Kết quả quá deadline hoặc không còn hợp lệ thì fallback/retry trong ngân sách. Định nghĩa rõ thời điểm bắt đầu lifetime (đề xuất sau admission) và giữ đúng workload được gọi tới mạng giữa các solver.

## Exact và optimality phải có phạm vi rõ

- Enumeration toàn bộ assignment trong U với candidate cố định chỉ là exact cho **neighborhood đó**, không phải tối ưu toàn SFC trên toàn mạng.
- Nếu mọi assignment được chấm qua cùng deterministic routing policy, có thể tìm nghiệm tốt nhất trong policy đó, nhưng chưa tối ưu mọi phương án routing.
- CP-SAT có thể bổ sung cho subproblem lớn hơn và reference joint model nhỏ. Dùng số nguyên/fixed precision được khai báo; ghi FEASIBLE/OPTIMAL/UNKNOWN objective bound và timeout. OPTIMAL chỉ áp dụng cho formulation đã giải.
- Cần một exact model joint placement-routing trên mạng nhỏ nếu muốn báo gap tới optimum toàn bài toán. Optimality của surrogate QUBO không thay thế được.

## Thí nghiệm cần có

1. HRE, FFD, Best-Fit hiện tại, không candidate reduction.
2. Cùng decomposition + exact/SA classical, cùng validator.
3. Static hybrid: luôn gọi quantum khi instance vừa ngân sách, cùng sampler.
4. Adaptive hybrid: lựa chọn backend theo tình trạng.
5. Ablation thay quantum bằng SA/exact nhưng giữ reducer, policy features và ngân sách tương ứng; đo cả trường hợp tắt repair để tách đóng góp classical.
6. Oracle chọn backend tốt nhất sau khi chạy mọi backend, chỉ làm upper reference offline và tuyệt đối không dùng như chính sách online.

Hai chế độ benchmark: (a) cùng wall-clock deadline/controller workers; (b) quality versus tổng chi phí CPU/QPU.

Đo acceptance và request admitted trước deadline; violation rate sau validation; energy/server/network; data-plane latency; p50/p95/p99 admission delay; số call và tổng QPU/CPU time; chi phí; feasible sample rate trước/sau repair; tỷ lệ rescue request, chất lượng incumbent cải thiện; qubit/coupler/depth; candidate pruning loss.

Giả thuyết đóng góp: adaptive policy giảm các lần gọi backend đắt tiền so với static hybrid mà vẫn giữ chất lượng, và decomposition tạo trade-off chất lượng/ chi phí có thể kiểm soát.

## Điểm tích hợp vào repository

Các module dưới đây đã được triển khai; chi tiết hành vi thực nằm trong hướng dẫn chạy:

- `virne/distributed/hybrid/candidates.py`: DC ranking, candidate reduction, neighborhood.
- `virne/distributed/hybrid/qubo.py`: BQM, coefficient normalization, decode và kiểm tra.
- `virne/distributed/hybrid/backends.py`: protocol cho exact/SA/QA/QAOA.
- `virne/distributed/hybrid/policy.py`: rule-based policy rồi contextual bandit.
- `virne/distributed/hybrid/solver.py`: giữ cùng hợp đồng solve → Allocation/reason.
- Refactor validator/routing dùng chung từ HeuristicSolver, giữ nguyên semantics của các baseline cũ. Substrate.commit/release tiếp tục quản lý trạng thái thật.
- `config.py`: thêm schema hybrid strict, backend mặc định classical, budget rõ.
- `runner.py`: log từng lần gọi backend và event decision_complete ở giai đoạn 2.

Các YAML đã có trong `configs/hybrid/` (remote QA mặc định disabled):
`distributed_ahq_adaptive.yaml`, `distributed_hybrid_exact.yaml`, `distributed_hybrid_sa.yaml`,
`distributed_hybrid_static_qa.yaml`, `distributed_hybrid_adaptive_qa.yaml`,
`distributed_hybrid_qaoa_sim.yaml`.

Có thêm `distributed_hybrid_learned.yaml` với model offline minh họa. Backend QA chưa chạy phần cứng; QAOA hiện dùng statevector NumPy, không chạy QPU.

Thứ tự triển khai: corre  ctness + classical controls → budgeted event orchestration → quantum backend và ablations → policy học được → migration/cross-DC nếu cần. Không cần chứng minh quantum speedup để đánh giá một kiến trúc điều phối, nhưng không thể tuyên bố thành phần quantum hữu ích nếu chưa vượt classical replacement.

## References

- [Lau, Beyond QAOA, arXiv:2610.11759v2](https://arxiv.org/html/2610.11759v2): tổng quan nhấn mạnh selective invocation, decomposition và đánh giá toàn workflow;chưa tìm thấy bằng chứng practical advantage thuyết phục ở mức cao nhất trong corpus khảo sát.
- [D-Wave Ocean stack](https://docs.dwavequantum.com/en/latest/ocean/stack.html): API sampler/BQM cho backend classical, QPU và hybrid.
- [D-Wave QPU parameters](https://docs.dwavequantum.com/en/latest/quantum_research/solver_parameters.html): embedding có thể dùng nhiều qubit physical cho một biến logical.
- [IBM QAOA](https://quantum.cloud.ibm.com/docs/en/tutorials/quantum-approximate-optimization-algorithm): workflow tối ưu tham số classical và thực thi circuit/sampling.
- [OR-Tools CP-SAT](https://developers.google.com/optimization/cp/cp_solver): ràng buộc integer và phân biệt solver status/optimality.
