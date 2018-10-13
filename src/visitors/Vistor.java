package visitors;

import operators.ScanOperator;

/**
 * this interface provides interface for Visitor
 *
 * @author Yixuan Jiang
 */
public interface Vistor {
	void visit(ScanOperator op);

}
