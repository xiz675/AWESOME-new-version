/* Generated By:JJTree: Do not edit this line. ASTAWSMFunction.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTAWSMFunction extends SimpleNode {
  public ASTAWSMFunction(int id) {
    super(id);
  }

  public ASTAWSMFunction(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=5d9037fb476d77cd093b88e4b1eb61e6 (do not edit this line) */
