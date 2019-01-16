package csi311;

import java.io.Serializable;
import java.util.List;

@SuppressWarnings("serial")
public class MachineSpec implements Serializable {

	// Since defined as an inner class, must be declared static or Jackson can't deal.
	public static class StateTransitions implements Serializable {
		private String state; 
		private List<String> transitions;
		public StateTransitions() { }
		public String getState() { return state; }
		public void setState(String state) { this.state = state.toLowerCase(); } 
		public List<String> getTransitions() { return transitions; } 
		public void setTransitions(List<String> transitions) { 
			this.transitions = transitions;
			if (this.transitions != null) {
				for (int i = 0; i < transitions.size(); i++) {
					transitions.set(i, transitions.get(i).toLowerCase()); 
				}
			}
		} 
	}
	
	private Integer tenantId;  
	private List<StateTransitions> machineSpec;
	public MachineSpec() { }
	public Integer getTenantId() { return tenantId; }
	public void setTenantId(Integer tenantId) { this.tenantId = tenantId; } 
	public List<StateTransitions> getMachineSpec() { return machineSpec; } 
	public void setMachineSpec(List<StateTransitions> machineSpec) { this.machineSpec = machineSpec; }
	
	
	public boolean stateTransitionsContain(String state1, String state2) {
		List<String> transitions = null; 
		for (StateTransitions sts : getMachineSpec()) {
			if (sts.getState().equals(state1)) {
				transitions = sts.getTransitions();
				break;
			}
		}
		if (transitions == null) {
			return false;
		}
		return transitions.contains(state2);
	}

	
	public static boolean isValidTransition(MachineSpec spec, String state1, String state2, boolean isNew) {
		if (isNew) {
			if (!spec.stateTransitionsContain("start", state1)) {
				return false; 
			}
		}
		return spec.stateTransitionsContain(state1, state2); 
	}
	
	
	public static boolean isTerminalState(MachineSpec spec, String state) {
		for (StateTransitions sts : spec.getMachineSpec()) {
			if (sts.getState().equals(state)) {
				if (sts.getTransitions().size() > 1) {
					return false; 
				}
				if (sts.getTransitions().get(0).equals(state)) {
					// transition to itself is allowed 
					return true; 
				}
			}
		}
		return true; 
	}
	
}


