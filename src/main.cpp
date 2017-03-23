#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <caf/crdt/all.hpp>

using namespace caf;
using namespace caf::crdt;
using namespace caf::crdt::types;


namespace {

struct port_dummy : public event_based_actor {
  using event_based_actor::event_based_actor;
};

using sub_atom = atom_constant<atom("sub")>;
using go_atom  = atom_constant<atom("go")>;

struct config : public crdt_config {
  config() : crdt_config() {
    load<io::middleman>();
    add_crdt<gset<std::string>>("bench");
  }
};

///
class master : public event_based_actor {
public:
  master(actor_config& cfg, uint32_t num_nodes, uint32_t num_workers, uint32_t num_per_worker)
    : event_based_actor(cfg), num_nodes_{num_nodes}, num_workers_{num_workers}, num_per_worker_{num_per_worker},
      items_{this, "bench"} {
    // nop
  }

protected:
  virtual behavior make_behavior() override {
    return {
      [&](sub_atom, const actor& sub) {
        workers_.emplace(sub);
        if (workers_.size() == num_workers_) {
          for (auto& w : workers_)
            send(w, go_atom::value);
          begin_ = std::chrono::high_resolution_clock::now();
        }
      },
      [&](notify_atom, const gset<std::string>& delta) {
        items_.merge(delta);
        if (items_.size() == num_nodes_ * num_per_worker_ * num_workers_) {
          end_ = std::chrono::high_resolution_clock::now();
	  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_ - begin_);
	  std::cout << diff.count() << std::endl;
          quit();
	}
      }
    };
  }
private:
  std::chrono::high_resolution_clock::time_point begin_;
  std::chrono::high_resolution_clock::time_point end_;
  uint32_t num_nodes_;
  uint32_t num_workers_;
  uint32_t num_per_worker_;
  std::set<actor> workers_;
  gset<std::string> items_;
};

///
class generator : public event_based_actor {
public:
  generator(actor_config& cfg, actor master, uint32_t to_generate, const std::string& id)
    : event_based_actor(cfg), generated_{0}, to_generate_{to_generate},
      id_{id}, items_{this, "bench"} {
    send(master, sub_atom::value, this);
  }

protected:
  virtual behavior make_behavior() override {
    return {
      [&](go_atom& atm) {
	if (generated_ >= to_generate_) {
	  quit();
	  return;
	}
	std::stringstream ss;
	ss << id_ << generated_++;
	items_.insert(ss.str());
	send(this, atm);
      },
      [&](notify_atom, const gset<std::string>& delta) {
        items_.merge(delta);
      }
    };
  }

private:
  uint32_t generated_;
  uint32_t to_generate_;
  std::string id_;
  gset<std::string> items_;
};

} // namespace <anonymous>

int main(int argc, char** argv) {
  auto print_help = []{
    std::cout << "Usage: ./crdt_bench num_nodes num_workers "
	      << "num_entries num_entries_per_worker [IPs...]"
	      << "local_hostname" << std::endl;
    std::exit(EXIT_FAILURE);
  };
  if (argc < 4) print_help();
  auto num_nodes   = static_cast<uint32_t>(std::stoi(argv[1]));
  auto num_workers = static_cast<uint32_t>(std::stoi(argv[2]));
  auto num_entries = static_cast<uint32_t>(std::stoi(argv[3]));
  std::string host_name = argv[num_nodes + 4];
  if (argc != 5 + num_nodes) print_help();
  std::vector<std::string> ips;
  for (int i = 4; i < argc; ++i)
    ips.emplace_back(argv[i]);
  // Init cfg & actor system
  config cfg;
  actor_system system{cfg};
  // Open local port with dummy
  system.middleman().publish(system.spawn<port_dummy>(), 1234);
  // Sleep some time, all instances need to open the port
  std::this_thread::sleep_for(std::chrono::seconds(2));  
  for (auto& ip : ips) {
    scoped_actor self{system};
    self->send(system.middleman().actor_handle(), connect_atom::value,
               ip, uint16_t{1234});
  }
  // Init local actors
  auto master_actor = system.spawn<master>(num_nodes, num_workers, num_entries);
  for (uint32_t i = 0; i < num_workers; ++i) {
    std::stringstream ss;
    ss << host_name << "-w-" << i << "-";
    system.spawn<generator>(master_actor, num_entries, ss.str());
  }
}

