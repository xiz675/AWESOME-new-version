/* Generated By:JJTree: Do not edit this line. ASTCreateAnalysis.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTCreateAnalysis extends SimpleNode {
  public ASTCreateAnalysis(int id) {
    super(id);
  }

  public ASTCreateAnalysis(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=6f59429d5b0a6f273b61ff156aa60ffa (do not edit this line) */