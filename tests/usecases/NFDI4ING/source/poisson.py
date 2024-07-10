"""
solution of the poisson equation on the unit square
"""

from argparse import ArgumentParser
import dolfin as df


def boundary_expression():
    """Defines the function to be used for the boundary conditions"""
    return "1.0 + x[0] * x[0] + 2.0 * x[1] * x[1]"


def solve_poisson(
    meshfile: str, degree: int, bc_expression: str = boundary_expression()
):
    """solves the poisson equation

    Parameters
    ----------
    meshfile : str
        FilePath to the mesh in xdmf format.
    degree : int
        Degree of the finite element space.

    Returns
    -------
    solution : df.Function
    """
    mesh = df.Mesh()
    with df.XDMFFile(meshfile) as instream:
        instream.read(mesh)
    func_space = df.FunctionSpace(mesh, "CG", degree)
    boundary_data = df.Expression(bc_expression, degree=2)

    def boundary(_, on_boundary):
        return on_boundary

    boundary_conditions = df.DirichletBC(func_space, boundary_data, boundary)
    trial_function = df.TrialFunction(func_space)
    test_function = df.TestFunction(func_space)
    source = df.Constant(-6.0)
    lhs = df.dot(df.grad(trial_function), df.grad(test_function)) * df.dx
    rhs = source * test_function * df.dx

    solution = df.Function(func_space)
    df.solve(lhs == rhs, solution, boundary_conditions)
    return solution


def solve_and_write_output(
    mesh: str, degree: int, outputfile: str, numdofs=None, return_dofs=False
):
    """solves the poisson equation and writes the solution
    and the number of degrees of freedom to the given file

    Parameters
    ----------
    meshfile : str
        FilePath to the mesh in xdmf format.
    degree : int
        Degree of the finite element space.
    outputfile : str
        FilePath to the output file into which the solution is written.
    numdofs : optional, str
        FilePath to which the number of degrees of freedom is written.
    return_dofs : optional, bool
        If True, return number of degrees of freedom.
    """
    discrete_solution = solve_poisson(mesh, degree)
    discrete_solution.rename("u", discrete_solution.name())
    resultFile = df.File(outputfile)
    resultFile << discrete_solution

    dofs = discrete_solution.function_space().dim()
    print(f"Number of dofs used: {dofs}")

    if numdofs is not None:
        with open(numdofs, "w") as handle:
            handle.write("{}\n".format(dofs))
    if return_dofs:
        return dofs


if __name__ == "__main__":
    PARSER = ArgumentParser(description="run script for the poisson problem")
    PARSER.add_argument("-m", "--mesh", required=True, help="mesh file to be used")
    PARSER.add_argument(
        "-d", "--degree", required=True, help="polynomial order to be used"
    )
    PARSER.add_argument(
        "-o",
        "--outputfile",
        required=True,
        help="file name for the output to be written",
    )
    PARSER.add_argument(
        "-n",
        "--num-dofs",
        required=False,
        default=None,
        help="file name for the number of DoFs to be written",
    )
    ARGS = vars(PARSER.parse_args())

    solve_and_write_output(
        ARGS["mesh"], int(ARGS["degree"]), ARGS["outputfile"], ARGS["num_dofs"]
    )
